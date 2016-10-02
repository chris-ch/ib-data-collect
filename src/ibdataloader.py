import logging
import re
from operator import itemgetter
from urllib.parse import parse_qs, urlparse

import pandas
from bs4 import BeautifulSoup

from urlcaching import open_url, invalidate_key

_PRODUCT_TYPES = {
    'Stock': 'stk',
    'Option': 'opt',
    'Future': 'fut',
    'Futures Option': 'fop',
    'ETF': 'etf',
    'Warrant': 'war',
    'Structured Product': 'iop',
    'Single Stock Future': 'ssf',
    'Forex': 'fx',
    'Metals': 'cmdty',
    'Index': 'ind',
    'Fund': 'mf',
    'CFD': 'cfd',
}
_BASE_URL = 'https://www.interactivebrokers.com'


def get_product_type_codes():
    return set(_PRODUCT_TYPES.values())


def get_product_type_code(product_type_name):
    return _PRODUCT_TYPES[product_type_name]


def get_product_type_names():
    return _PRODUCT_TYPES.keys()


def load_exchanges_for_product_type(product_type_code):
    url_template = _BASE_URL + '/en/index.php?f=products&p={product_type_code}'
    url = url_template.format(product_type_code=product_type_code)
    html_text = open_url(url, rejection_marker='To continue please enter', throttle=3)
    html = BeautifulSoup(html_text, 'html.parser')
    region_list_tag = html.find('div', {'id': product_type_code})
    if region_list_tag is None:
        region_urls = {'unknown': url}

    else:
        region_urls = {region_link_tag.string: _BASE_URL + region_link_tag['href']
                       for region_link_tag in region_list_tag.find_all('a')
                       }

    exchanges = list()
    for region_name in region_urls:
        region_url = region_urls[region_name]
        html_exchanges_text = open_url(region_url, rejection_marker='To continue please enter', throttle=3)
        html_exchanges = BeautifulSoup(html_exchanges_text, 'html.parser')
        exchanges_region = list()
        for link_tag in html_exchanges.find_all('a'):
            if link_tag.get('href') and link_tag.get('href').startswith('index.php?f='):
                exchange_name = link_tag.string.encode('ascii', 'ignore').decode().strip()
                exchange_url = _BASE_URL + '/en/' + link_tag['href']
                exchanges_region.append((exchange_name, exchange_url))

        exchanges += exchanges_region

    return exchanges


def load_for_exchange_partial(exchange_name, exchange_url):
    instruments = list()
    html_text = open_url(exchange_url, rejection_marker='To continue please enter', throttle=3)

    def find_stock_details_link(tag):
        is_link = tag.name == 'a'
        if is_link and 'href' in tag.attrs:
            return tag['href'].startswith("javascript:NewWindow('https://misc.interactivebrokers.com/cstools")

        return False

    next_page_url = None
    try:
        html = BeautifulSoup(html_text, 'lxml')
        pagination_tag = html.find('ul', {'class': 'pagination'})
        if pagination_tag is not None:
            current_page_tag = pagination_tag.find('li', {'class': 'active'})
            if current_page_tag is not None:
                next_page_tag = current_page_tag.find_next_sibling()
                if next_page_tag:
                    next_page_url = _BASE_URL + next_page_tag.find('a').get('href')

        stock_link_tags = html.find_all(find_stock_details_link)
        for tag in stock_link_tags:
            url = re.search(r"javascript:NewWindow\('(.*?)',", tag['href']).group(1)
            query = parse_qs(urlparse(url).query)
            if 'conid' in query.keys():
                instrument_data = dict(conid=query['conid'][0], label=tag.string, exchange=exchange_name)
                tag_row = tag.parent.parent
                instrument_tags = [tag.string for tag in tag_row.find_all('td')]
                if len(instrument_tags) != 4:
                    raise RuntimeError('Unexpected instrument tags found: %s', instrument_tags)

                ib_symbol, url_text, symbol, currency = instrument_tags

                instrument_data['ib_symbol'] = ib_symbol
                instrument_data['symbol'] = symbol
                instrument_data['currency'] = currency
                instruments.append(instrument_data)

    except Exception:
        logging.error('failed to load exchange "%s"', exchange_name, exc_info=True)
        invalidate_key(exchange_url)
        raise

    return instruments, next_page_url


def load_for_exchange(exchange_name, exchange_url):
    """

    :param exchange_name:
    :param exchange_url:
    :return: list of dict
    """
    instruments = list()
    next_page_link = exchange_url
    while True:
        new_instruments, next_page_link = load_for_exchange_partial(exchange_name, next_page_link)
        instruments += new_instruments
        if next_page_link is None:
            break

    return instruments


# noinspection PyTypeChecker
def list_instruments(product_type_codes):
    for product_type_code in sorted(product_type_codes):
        exchanges = load_exchanges_for_product_type(product_type_code)
        logging.info('%d available exchanges for product type "%s"', len(exchanges), product_type_code)
        for exchange_name, exchange_url in sorted(exchanges, key=itemgetter(0)):
            logging.info('processing exchange data %s, %s', exchange_name, exchange_url)
            exchange_instruments = load_for_exchange(exchange_name, exchange_url)
            for instrument in exchange_instruments:
                instrument['product_type_code'] = product_type_code
                yield instrument


def process_instruments(product_type_codes, results_processor, limit=None):
    """

    :param product_type_codes:
    :param results_processor: function taking (product_type_code, currency, instruments dataframe) as input
    :param limit: limits the number of instruments to process (dev only)
    :return:
    """
    rows = list()
    for count, row in enumerate(list_instruments(product_type_codes)):
        rows.append(row)
        if count == limit:
            break

    df = pandas.DataFrame(rows)
    compact_df = df.groupby(['product_type_code', 'currency', 'conid', 'symbol', 'ib_symbol', 'label']).count().reset_index()
    compact_df.drop('exchange', axis=1, inplace=True)
    by_currency = compact_df.groupby(['product_type_code', 'currency'])
    for key, instruments_df in by_currency:
        product_type_code, currency = key
        currency_instruments = instruments_df.drop('currency', axis=1).drop('product_type_code', axis=1)
        results_processor(product_type_code, currency, currency_instruments)