import argparse
import logging
import os
import re
from operator import itemgetter
from pprint import pprint
from time import sleep
from urllib.parse import urlparse
from urllib.parse import parse_qs
import pandas
from bs4 import BeautifulSoup

from urlcaching import set_cache_path, open_url, invalidate_key

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


def load_exchanges_for_product_type(product_type_code):
    url_template = _BASE_URL + '/en/index.php?f=products&p={product_type_code}'
    url = url_template.format(product_type_code=product_type_code)
    html_text = open_url(url, rejection_marker='To continue please enter', throttle=1)
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
        html_exchanges_text = open_url(region_url, rejection_marker='To continue please enter', throttle=1)
        html_exchanges = BeautifulSoup(html_exchanges_text, 'html.parser')
        exchanges_region = list()
        for link_tag in html_exchanges.find_all('a'):
            if link_tag.get('href') and link_tag.get('href').startswith('index.php?f='):
                exchange_name = ' '.join(link_tag.string.split()[:-1])
                exchange_code = link_tag.string.split()[-1]
                exchange_url = _BASE_URL + '/en/' + link_tag['href']
                if exchange_name == '':
                    exchange_name = exchange_code

                exchanges_region.append((exchange_name, exchange_code, exchange_url))

        exchanges += exchanges_region

    return exchanges


def load_for_exchange_partial(exchange_full_name, exchange_url):
    instruments = list()
    html_text = open_url(exchange_url, rejection_marker='To continue please enter', throttle=1)

    def find_stock_details_link(tag):
        is_link = tag.name == 'a'
        if is_link and 'href' in tag.attrs:
            return tag['href'].startswith("javascript:NewWindow('https://misc.interactivebrokers.com/cstools")

        return False

    next_page_url = None
    try:
        html = BeautifulSoup(html_text, 'html.parser')
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
                exchange_fields = exchange_full_name.split()
                exchange_name = ' '.join(exchange_fields[:-1])
                exchange_code = exchange_fields[-1][1:-1]
                instrument_data = dict(conid=query['conid'][0], label=tag.string, exchange=exchange_name,
                                       exchange_code=exchange_code)
                instruments.append(instrument_data)

    except Exception:
        logging.error('failed to load exchange "%s"', exchange_full_name, exc_info=True)
        invalidate_key(exchange_url)
        raise

    return instruments, next_page_url


def load_for_exchange(exchange_name, exchange_url):
    instruments = list()
    next_page_link = exchange_url
    while True:
        new_instruments, next_page_link = load_for_exchange_partial(exchange_name, next_page_link)
        instruments += new_instruments
        if next_page_link is None:
            break

    return instruments


def main(args):
    if args.list_product_types:
        print('Available product types:')
        for product in _PRODUCT_TYPES:
            print(' - {} ({})'.format(_PRODUCT_TYPES[product], product))

        return

    product_type_codes = _PRODUCT_TYPES.values()
    if args.product_type:
        product_type_codes = [args.product_type]

    instruments = list()
    for product_type_code in sorted(product_type_codes):
        exchanges = load_exchanges_for_product_type(product_type_code)
        for exchange_name, exchange_code, exchange_url in sorted(exchanges, key=itemgetter(0)):
            logging.info('processing exchange data %s, %s, %s', exchange_name, exchange_code, exchange_url)
            instruments += load_for_exchange(exchange_name, exchange_url)

    output_file = os.sep.join([args.output_dir, args.output_name + '.xlsx'])
    logging.info('saving to file %s', os.path.abspath(output_file))
    writer = pandas.ExcelWriter(output_file)
    instruments_df = pandas.DataFrame(instruments).sort_values(by='label')
    instruments_df.to_excel(writer, 'instruments', index=False)
    writer.save()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output-name', type=str, help='name of the output file', default='ib-instr')
    parser.add_argument('--list-product-types', dest='list_product_types', action='store_true',
                        help='only displays available product types')
    parser.add_argument('product_type', type=str, choices=_PRODUCT_TYPES.values(), nargs='?',
                        help='limits download to specified product type')
    args = parser.parse_args()
    set_cache_path(os.path.sep.join([args.output_dir, 'ib-instr-urlcaching']))
    main(args)

