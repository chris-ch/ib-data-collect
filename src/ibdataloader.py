import logging
import re
from collections import defaultdict
from enum import unique, StrEnum
from operator import itemgetter
from typing import Iterable, Callable, Generator, Tuple, List
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from webscrapetools import urlcaching

_URL_BASE = 'https://www.interactivebrokers.com'
_EXCHANGES_REJECTION_MARKER = 'To continue please enter'
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


def load_url(url: str, rejection_marker=None) -> str:
    if not rejection_marker:
        rejection_marker = _EXCHANGES_REJECTION_MARKER

    html_text = urlcaching.open_url(url, rejection_marker=rejection_marker, throttle=3)
    return html_text


def notify_url_error(url: str) -> None:
    logging.error('failed to load url: {}'.format(url))
    urlcaching.invalidate_key(url)


def load_exchanges_for_product_type(product_type: ProductType) -> List[List[Tuple[str, str]]]:
    url = _URL_BASE + f'/en/index.php?f=products&p={product_type.value}'
    logging.info(f'loading data for product type {product_type.value}: {url}')
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
                exchange_url = _URL_BASE + f"/en/{link_tag['href']}"
                logging.info(f'found url for exchange {exchange_name}: {exchange_url}')
                exchanges_region.append((exchange_name, exchange_url))

        exchanges += exchanges_region

    return exchanges


class AsDict(object):

    def as_dict(self):
        return {field: self.__getattribute__(field) for field in self.__dir__()
                if not field.startswith('_') and field not in ('as_dict', 'fields')}


class Instrument(AsDict):

    def __init__(self, con_id: str, label: str, exchange: str):
        self._con_id = con_id
        self._label = label
        self._exchange = exchange
        self._symbol = None
        self._ib_symbol = None
        self._currency = None
        self._product_type = None

    @property
    def con_id(self) -> str:
        return self._con_id

    @property
    def label(self) -> str:
        return self._label

    @property
    def symbol(self) -> str:
        return self._symbol

    @symbol.setter
    def symbol(self, value: str)-> None:
        self._symbol = value

    @property
    def ib_symbol(self) -> str:
        return self._ib_symbol

    @ib_symbol.setter
    def ib_symbol(self, value: str)-> None:
        self._ib_symbol = value

    @property
    def currency(self) -> str:
        return self._currency

    @currency.setter
    def currency(self, value: str) -> None:
        self._currency = value

    @property
    def product_type(self) -> ProductType:
        return self._product_type

    @product_type.setter
    def product_type(self, value: ProductType) -> None:
        self._product_type = value


def load_for_exchange_partial(exchange_name: str, exchange_url: str) -> Tuple[List[Instrument], str]:
    instruments = list()
    html_text = load_url(exchange_url)

    rule_contract_url = re.compile(r"javascript:NewWindow\(\'(.*?)\'")

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
                instrument = Instrument(con_id=query['conid'][0], label=tag.string, exchange=exchange_name)
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
        notify_url_error(exchange_url)
        raise

    return instruments, next_page_url


def load_for_exchange(exchange_name: str, exchange_url: str) -> List[Instrument]:
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
        for exchange_name, exchange_url in sorted(exchanges, key=itemgetter(0)):
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

    for product_type, currency in by_product_type_and_currency:
        instruments = by_product_type_and_currency[(product_type, currency)]
        results_processor(product_type, currency, sorted(instruments, key=lambda k: k.label.upper()))

