import argparse
import csv
import logging
import os
import sys

import ibdataloader
from webscrapetools.urlcaching import set_cache_path


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output-prefix', type=str, help='prefix for the output files', default='ib-instr')
    parser.add_argument('--list-product-types', action='store_true', help='only displays available product types')
    parser.add_argument('--use-cache', type=str, help='directory for caching web requests', default=None)
    parser.add_argument('product_types', type=str, nargs='*',
                        help='download specified product types, or all if not specified')
    args = parser.parse_args()

    if args.list_product_types:
        print('Available product types:')
        for product in ibdataloader.get_product_type_names():
            print(' - {} ({})'.format(ibdataloader.get_product_type_code(product), product))

        return

    if args.use_cache:
        cache_path = os.path.abspath(os.path.sep.join([args.use_cache, 'ib-instr-urlcaching']))
        logging.info('using cache %s for web requests', cache_path)
        set_cache_path(cache_path, expiry_days=2)

    product_type_codes = set(args.product_types)
    if not product_type_codes.issubset(ibdataloader.get_product_type_codes()):
        allowed_types = ibdataloader.get_product_type_codes()
        logging.error('some instrument types are not defined: %s', product_type_codes.difference(allowed_types))
        sys.exit(0)

    if not product_type_codes:
        product_type_codes = ibdataloader.get_product_type_codes()

    logging.info('loading product types {}'.format(product_type_codes))

    # noinspection PyTypeChecker
    def results_writer(product_type_code, currency, instruments):
        # saving to local drive
        logging.info('saving results to %s', os.path.abspath(args.output_dir))
        os.makedirs(args.output_dir, exist_ok=True)
        output_filename = args.output_prefix + '-' + currency.lower() + '-' + product_type_code.lower() + '.csv'
        output_path = os.path.abspath(os.sep.join((args.output_dir, output_filename)))
        header = ('conid', 'symbol', 'ib_symbol', 'label')
        with open(output_path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()
            for instrument in instruments:
                writer.writerow(instrument)

        logging.info('saved file: %s', output_path)

    ibdataloader.process_instruments(product_type_codes, results_writer)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logname = os.path.abspath(sys.argv[0]).split(os.sep)[-1].split(".")[0]
    file_handler = logging.FileHandler(logname + '.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()

    except SystemExit:
        pass
    except:
        logging.exception('error occured', sys.exc_info()[0])
        raise
