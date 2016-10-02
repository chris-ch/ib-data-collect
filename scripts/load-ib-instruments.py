import argparse
import logging
import os

import sys

import ibdataloader
from urlcaching import set_cache_path


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output-prefix', type=str, help='prefix for the output files', default='ib-instr')
    parser.add_argument('--list-product-types', action='store_true', help='only displays available product types')
    parser.add_argument('--use-cache', action='store_true', help='caches web requests (for dev only)')
    parser.add_argument('product_types', type=str, nargs='*',
                        help='download specified product types, or all if not specified')
    args = parser.parse_args()
    if args.use_cache:
        set_cache_path(os.path.sep.join([args.output_dir, 'ib-instr-urlcaching']))

    if args.list_product_types:
        print('Available product types:')
        for product in ibdataloader.get_product_type_names():
            print(' - {} ({})'.format(ibdataloader.get_product_type_code(product), product))

        return

    product_type_codes = set(args.product_types)
    if not product_type_codes.issubset(ibdataloader.get_product_type_codes()):
        allowed_types = ibdataloader.get_product_type_codes()
        logging.error('some instrument types are not defined: %s', product_type_codes.difference(allowed_types))
        sys.exit(0)

    if not product_type_codes:
        product_type_codes = ibdataloader.get_product_type_codes()

    logging.info('loading %s', product_type_codes)

    def results_printer(currency, instruments_df):
        print(currency)
        print(instruments_df)

    ibdataloader.process_instruments(product_type_codes, results_printer, limit=30)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    main()

