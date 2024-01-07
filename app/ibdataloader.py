import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from enum import unique, StrEnum
from functools import lru_cache
from operator import itemgetter
from typing import Iterable, Callable, Generator, Tuple, List, Set, NamedTuple
from urllib.parse import parse_qs, urlparse

from webscrapetools import urlcaching
from bs4 import BeautifulSoup

_URL_BASE = 'https://www.interactivebrokers.com'
_URL_CONTRACT_DETAILS = 'https://contract.ibkr.info/index.php'


@unique
class ProductType(StrEnum):
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

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    def __str__(self):
        return self.value


class Exchange(NamedTuple):
    name: str
    url: str


def load_exchanges_for_product_type(product_type: ProductType) -> List[Exchange]:
    url = f'{_URL_BASE}/en/index.php?f=products&p={product_type.value}'
    logging.info(f'loading data for product type {product_type.value}: {url}')
    response = urlcaching.open_url(url)
    html = BeautifulSoup(response, 'html.parser')
    region_list_tag = html.find('div', {'id': product_type.value})
    if region_list_tag is None:
        region_urls = {'unknown': url}

    else:
        region_urls = {region_link_tag.string: _URL_BASE + region_link_tag['href']
                       for region_link_tag in region_list_tag.find_all('a')
                       }

    exchanges = set()
    for region_url in region_urls.values():
        exchanges.update(load_exchanges_for_region_url(region_url))

    return sorted(exchanges)


@lru_cache(maxsize=None)
def load_exchanges_for_region_url(region_url: str) -> Set[Exchange]:
    response_exchanges = urlcaching.open_url(region_url)
    html_exchanges = BeautifulSoup(response_exchanges, 'html.parser')
    exchanges_region = set()
    for link_tag in html_exchanges.find_all('a'):
        if link_tag.get('href') and link_tag.get('href').startswith('index.php?f='):
            exchange_name = str(link_tag.contents[0]).encode('ascii', 'ignore').decode().strip()
            exchange_url = f"{_URL_BASE}/en/{link_tag['href']}"
            logging.debug(f'found url for exchange {exchange_name}: {exchange_url}')
            exchanges_region.add(Exchange(name=exchange_name, url=exchange_url))
    return exchanges_region


@dataclass
class Instrument:
    con_id: str
    label: str
    exchange: str
    symbol: str = None
    ib_symbol: str = None
    currency: str = None
    product_type: ProductType = None


def load_for_exchange_partial(exchange_name: str, exchange_url: str) -> Tuple[List[Instrument], str]:
    instruments = []
    response = urlcaching.open_url(exchange_url)

    rule_contract_url = re.compile(r"javascript:NewWindow\(\'(.*?)\'")

    def find_stock_details_link(a_tag):
        is_link = a_tag.name == 'a'
        if is_link and 'href' in a_tag.attrs:
            check_contract_url = rule_contract_url.match(a_tag['href'])
            return check_contract_url and check_contract_url[1].startswith(
                _URL_CONTRACT_DETAILS
            )

        return False

    next_page_url = None
    try:
        html = BeautifulSoup(response, 'lxml')
        pagination_tag = html.find('ul', {'class': 'pagination'})
        if pagination_tag is not None:
            current_page_tag = pagination_tag.find('li', {'class': 'active'})
            if current_page_tag is not None:
                next_page_tag = current_page_tag.find_next_sibling()
                if next_page_tag:
                    next_page_url = _URL_BASE + next_page_tag.find('a').get('href')

        stock_link_tags = html.find_all(find_stock_details_link)
        for tag in stock_link_tags:
            url = rule_contract_url.search(tag['href'])[1]
            query = parse_qs(urlparse(url).query)
            if 'conid' in query.keys():
                instrument = Instrument(con_id=query['conid'][0].strip(), label=tag.string.strip(), exchange=exchange_name.strip())
                tag_row = tag.parent.parent
                instrument_tags = [tag.string for tag in tag_row.find_all('td')]
                if len(instrument_tags) != 4:
                    raise RuntimeError('Unexpected instrument tags found: %s', instrument_tags)

                ib_symbol, url_text, symbol, currency = instrument_tags
                instrument.ib_symbol = ib_symbol
                instrument.symbol = symbol
                instrument.currency = currency
                instruments.append(instrument)

    except Exception:
        logging.error('failed to load exchange "%s"', exchange_name, exc_info=True)
        raise

    return instruments, next_page_url


def load_for_exchange(exchange_name: str, exchange_url: str) -> List[Instrument]:
    """

    :param exchange_name:
    :param exchange_url:
    :return: list of dict
    """
    instruments = []
    next_page_link = exchange_url
    while True:
        logging.info("processing page %s", next_page_link)
        new_instruments, next_page_link = load_for_exchange_partial(exchange_name, next_page_link)
        if len(new_instruments) > 0:
            logging.info(f'retrieved {len(new_instruments)} instruments from "{new_instruments[0].label}" through "{new_instruments[-1].label}"')

        instruments += new_instruments
        if next_page_link is None:
            break

    return instruments


def list_instruments(product_types: Iterable[ProductType]) -> Generator[Instrument, None, None]:
    """

    :param product_types:
    :return: dict() representing the instrument row
    """
    for product_type in sorted(product_types):
        exchanges = load_exchanges_for_product_type(product_type)
        logging.info(f'{len(exchanges)} available exchanges for product type "{product_type}"', )
        for exchange_name, exchange_url in exchanges:
            logging.info(f'processing exchange data {exchange_name}, {exchange_url}')
            exchange_instruments = load_for_exchange(exchange_name, exchange_url)
            for instrument in exchange_instruments:
                instrument.product_type = product_type
                yield instrument


def process_instruments(product_types: Iterable[ProductType],
                        results_processor: Callable[[ProductType, str, Iterable[Instrument]], None]) -> None:
    """

    :param product_types:
    :param results_processor: function taking (product_type_code, currency, instruments list) as input
    :return:
    """
    logging.info('processing instruments')
    by_product_type_and_currency = defaultdict(list)
    for instrument in list_instruments(product_types):
        by_product_type_and_currency[(instrument.product_type, instrument.currency)].append(instrument)

    for product_type, currency in by_product_type_and_currency.keys():
        instruments = by_product_type_and_currency[(product_type, currency)]
        results_processor(product_type, currency, sorted(instruments, key=lambda k: k.label.upper()))
