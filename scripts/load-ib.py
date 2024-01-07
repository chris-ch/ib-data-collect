import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from dataclasses import asdict
from typing import Iterable, Dict, List

import ibdataloader
from ibdataloader import Instrument, ProductType
from webscrapetools.urlcaching import set_cache_path

_FILENAME_SEPARATOR = '_'


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output-prefix', type=str, help='prefix for the output files', default='ib-instr')
    parser.add_argument('--list-product-types', action='store_true', help='only displays available product types')
    parser.add_argument('--list-exchanges', action='store_true', help='only displays exchanges')
    parser.add_argument('--use-cache', type=str, help='directory for caching web requests', default=None)
    parser.add_argument('--cache-expiry', type=int, help='number of days for cache expiry', default=20)
    parser.add_argument('product_types', type=str, nargs='*',
                        help='download specified product types, or all if not specified')
    args = parser.parse_args()

    if args.list_product_types:
        print('Available product types:')
        for product in ibdataloader.ProductType:
            print(' - {} ({})'.format(product.value, product.long_name()))

        return

    if args.list_exchanges:
        exchanges_products: Dict[str, List[ProductType]] = defaultdict(list)
        for product_type in ibdataloader.ProductType:
            if product_type == ProductType.FUND:
                continue
            for exchange in ibdataloader.load_exchanges_for_product_type(product_type):
                exchanges_products[exchange.name].append(product_type)
        print('Available exchanges:')
        for exchange_name in sorted(exchanges_products):
            print(f"{exchange_name}: ", end="")
            product_types: List[ProductType] = exchanges_products[exchange_name]
            for count, product_type in enumerate(product_types, 1):
                print(f"{product_type.long_name()} ({product_type.value})", end='')
                if count == len(product_types):
                    print()
                else:
                    print(', ', end='')
        return

    if args.use_cache:
        cache_path = os.path.abspath(os.path.sep.join([args.use_cache, 'ib-instr-urlcaching']))
        logging.info('using cache %s for web requests (expiring after %d days)', cache_path, args.cache_expiry)
        set_cache_path(cache_path, expiry_days=args.cache_expiry)

    product_type_codes = set(args.product_types)
    if not product_type_codes.issubset(
        {prod_type.value for prod_type in ibdataloader.ProductType}
    ):
        allowed_types = {prod_type.value for prod_type in ibdataloader.ProductType}
        logging.error('some instrument types are not defined: %s', product_type_codes.difference(allowed_types))
        sys.exit(0)

    if not product_type_codes:
        product_types = list(ibdataloader.ProductType)

    else:
        product_types = [
            prod_type
            for prod_type in ibdataloader.ProductType
            if prod_type.value in product_type_codes
        ]

    logging.info('loading product types {}'.format(product_types))

    def results_writer(product_type: ProductType, currency: str, instruments: Iterable[Instrument]) -> None:
        # saving to local drive
        logging.info('saving results to %s', os.path.abspath(args.output_dir))
        os.makedirs(args.output_dir, exist_ok=True)
        output_filename = args.output_prefix + _FILENAME_SEPARATOR + currency.lower() + _FILENAME_SEPARATOR + product_type.value + '.csv'
        output_path = os.path.abspath(os.sep.join((args.output_dir, output_filename)))
        with open(output_path, 'w') as csv_file:
            for _count, instrument in enumerate(instruments, 1):
                if _count == 1:
                    writer = csv.DictWriter(csv_file, fieldnames=list(asdict(instrument).keys()))
                    writer.writeheader()

                else:
                    as_dict = asdict(instrument)
                    writer.writerow(as_dict)

        logging.info('saved file: %s', output_path)

    ibdataloader.process_instruments(product_types, results_writer)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logname = os.path.abspath(sys.argv[0]).split(os.sep)[-1].split(".")[0]
    file_handler = logging.FileHandler(f'{logname}.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()

    except SystemExit:
        pass
    except:
        logging.exception('error occurred', sys.exc_info()[0])
        raise
