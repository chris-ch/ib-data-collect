import logging
import re
import tempfile
import base64
from enum import Enum, unique
from operator import itemgetter
from typing import Iterable, Callable, Generator, Dict, Tuple, List
from urllib.parse import parse_qs, urlparse

import pickle
from bs4 import BeautifulSoup
from webscrapetools import urlcaching

_URL_BASE = 'https://www.interactivebrokers.com'
_URL_TEMPLATE_EXCHANGES = '/en/index.php?f=products&p={product_type_code}'
_EXCHANGES_REJECTION_MARKER = 'To continue please enter'
_URL_CONTRACT_DETAILS = 'https://contract.ibkr.info/index.php'


@unique
class ProductType(Enum):
    STOCK = 'stk'
    OPTION = 'opt'
    FUTURE = 'fut'
    FUTURES_OPTION = 'fop'
    ETF = 'etf'
    WARRANT = 'war'
    STRUCTURED_PRODUCT = 'iop'
    SINGLE_STOCK_FUTURE = 'ssf'
    FOREX = 'fx'
    METALS = 'cmdty'
    INDEX = 'ind'
    FUND = 'mf'
    CFD = 'cfd'

    def long_name(self):
        return self.name.replace('_', ' ').capitalize()


def load_url(url, rejection_marker=None):
    if not rejection_marker:
        rejection_marker = _EXCHANGES_REJECTION_MARKER

    html_text = urlcaching.open_url(url, rejection_marker=rejection_marker, throttle=3)
    return html_text


def notify_url_error(url):
    urlcaching.invalidate_key(url)


def load_exchanges_for_product_type(product_type: ProductType) -> List[List[Tuple[str, str]]]:
    url_template = _URL_BASE + _URL_TEMPLATE_EXCHANGES
    url = url_template.format(product_type_code=product_type.value)
    logging.info('loading data for product type %s: %s', product_type.value, url)
    html_text = load_url(url)
    html = BeautifulSoup(html_text, 'html.parser')
    region_list_tag = html.find('div', {'id': product_type.value})
    if region_list_tag is None:
        region_urls = {'unknown': url}

    else:
        region_urls = {region_link_tag.string: _URL_BASE + region_link_tag['href']
                       for region_link_tag in region_list_tag.find_all('a')
                       }

    exchanges = list()
    for region_name in region_urls:
        region_url = region_urls[region_name]
        html_exchanges_text = load_url(region_url)

        html_exchanges = BeautifulSoup(html_exchanges_text, 'html.parser')
        exchanges_region = list()
        for link_tag in html_exchanges.find_all('a'):
            if link_tag.get('href') and link_tag.get('href').startswith('index.php?f='):
                exchange_name = link_tag.string.encode('ascii', 'ignore').decode().strip()
                last_request = urlcaching.get_last_request()
                if last_request:
                    base_url = urlparse(last_request.url)
                    exchange_url = base_url.scheme + '://' + base_url.netloc + '/en/' + link_tag['href']

                else:
                    exchange_url = _URL_BASE + '/en/' + link_tag['href']

                logging.info('found url for exchange %s: %s', exchange_name, exchange_url)
                exchanges_region.append((exchange_name, exchange_url))

        exchanges += exchanges_region

    return exchanges


def load_for_exchange_partial(exchange_name: str, exchange_url : str) -> Tuple[List[Dict[str, str]], str]:
    instruments = list()
    html_text = load_url(exchange_url)

    rule_contract_url = re.compile(r"javascript:NewWindow\('(.*?)\)',")

    def find_stock_details_link(tag):
        is_link = tag.name == 'a'
        if is_link and 'href' in tag.attrs:
            check_contract_url = rule_contract_url.match(tag['href'])
            return check_contract_url and check_contract_url.group(1).startswith(_URL_CONTRACT_DETAILS)

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
                    next_page_url = _URL_BASE + next_page_tag.find('a').get('href')

        stock_link_tags = html.find_all(find_stock_details_link)
        for tag in stock_link_tags:
            url = rule_contract_url.search(tag['href']).group(1)
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
        notify_url_error(exchange_url)
        raise

    return instruments, next_page_url


def load_for_exchange(exchange_name: str, exchange_url: str) -> List[Dict[str, str]]:
    """

    :param exchange_name:
    :param exchange_url:
    :return: list of dict
    """
    instruments = list()
    next_page_link = exchange_url
    while True:
        logging.info('processing page %s', next_page_link)
        new_instruments, next_page_link = load_for_exchange_partial(exchange_name, next_page_link)
        if len(new_instruments) > 0:
            logging.info('retrieved %d instruments from "%s" through "%s"', len(new_instruments), new_instruments[0]['label'], new_instruments[-1]['label'])

        instruments += new_instruments
        if next_page_link is None:
            break

    return instruments


def list_instruments(product_types: Iterable[ProductType]) -> Generator[Dict[str, str], None, None]:
    """

    :param product_types:
    :return: dict() representing the instrument row
    """
    for product_type in sorted(product_types):
        exchanges = load_exchanges_for_product_type(product_type)
        logging.info('%d available exchanges for product type "%s"', len(exchanges), product_type)
        for exchange_name, exchange_url in sorted(exchanges, key=itemgetter(0)):
            logging.info('processing exchange data %s, %s', exchange_name, exchange_url)
            exchange_instruments = load_for_exchange(exchange_name, exchange_url)
            for instrument in exchange_instruments:
                instrument['product_type_code'] = product_type.value
                yield instrument


def process_instruments(product_types: Iterable[ProductType],
                        results_processor: Callable[[ProductType, str, Iterable[str]], None],
                        limit: int=None) -> None:
    """

    :param product_types:
    :param results_processor: function taking (product_type_code, currency, instruments list) as input
    :param limit: limits the number of instruments to process (dev only)
    :return:
    """
    instruments_db_files = dict()
    con_id_set = set()
    logging.info('processing instruments')
    instruments = list(list_instruments(product_types))
    for count, row in enumerate(instruments):
        product_type_code = row['product_type_code']
        currency = row['currency']
        con_id = row['conid']
        symbol = row['symbol']
        ib_symbol = row['ib_symbol']
        label = row['label']
        shelve_key = (ProductType(product_type_code), currency)
        if shelve_key not in instruments_db_files:
            instruments_db_files[shelve_key] = tempfile.TemporaryFile(mode='w+b')

        if con_id not in con_id_set:
            instrument_data = {'conid': str(con_id), 'symbol': str(symbol), 'ib_symbol': str(ib_symbol), 'label': str(label)}
            line = base64.b64encode(pickle.dumps(instrument_data))
            instruments_db_files[shelve_key].write(line + b'\n')
            con_id_set.add(con_id)

        if count == limit:
            break

    for key in instruments_db_files.keys():
        instruments_db_files[key].seek(0)
        currency_instruments = list()
        for line in instruments_db_files[key]:
            if len(line) > 0:
                logging.debug('unpickling "%s"', line)
                pickled = pickle.loads(base64.b64decode(line))
                currency_instruments.append(pickled)

        instruments_db_files[key].close()
        logging.info('processing %d instruments for %s', len(currency_instruments), key)
        product_type, currency = key
        results_processor(product_type, currency, sorted(currency_instruments, key=lambda k: k['label'].upper()))
