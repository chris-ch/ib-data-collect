import argparse
import csv
import json
import logging
import os
import sys

import oauth2client.tools

import ibdataloader
from gservices import setup_services, prepare_sheet, update_sheet
from urlcaching import set_cache_path

_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     parents=[oauth2client.tools.argparser]
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output-prefix', type=str, help='prefix for the output files', default='ib-instr')
    parser.add_argument('--list-product-types', action='store_true', help='only displays available product types')
    parser.add_argument('--use-cache', action='store_true', help='caches web requests (for dev only)')
    parser.add_argument('product_types', type=str, nargs='*',
                        help='download specified product types, or all if not specified')
    parser.add_argument('--config',
                        metavar='JSON_FILENAME',
                        type=str,
                        help='location of config file, using "{}" by default'.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    args = parser.parse_args()

    full_config_path = os.path.abspath(args.config)
    logging.info('reading from config "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    config_keys = ('api_key', 'google_api_client_secret', 'token_filename', 'target_folder_id', 'db_file_prefix')
    for config_key in config_keys:
        if config_key not in config_json.keys():
            raise RuntimeError('Key {} is missing from config file'.format(config_key))

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

    client_secret_filename = config_json['google_api_client_secret']
    api_key = config_json['api_key']
    token_filename = config_json['token_filename']
    drive, sheets = setup_services(client_secret_filename, api_key, token_filename, args)

    # noinspection PyTypeChecker
    def results_writer(product_type_code, currency, instruments):
        # saving to local drive
        output_filename = args.output_prefix + '-' + currency.lower() + '-' + product_type_code.lower() + '.csv'
        output_path = os.sep.join((args.output_dir, output_filename))
        header = ('conid', 'symbol', 'ib_symbol', 'label')
        with open(output_path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()
            for instrument in instruments:
                writer.writerow(instrument)

        logging.info('saved file: %s', output_path)

        # saving to Google drive
        sheet_name = '{} {}-{}'.format(config_json['db_file_prefix'], product_type_code.upper(), currency.upper())
        spreadsheet_id = prepare_sheet(drive, sheets, config_json['target_folder_id'], sheet_name)
        logging.info('prepared Google sheet %s: %s', sheet_name, spreadsheet_id)
        rows = instruments
        logging.info('saving %d instruments', len(rows))
        update_sheet(sheets, spreadsheet_id, header, rows)
        logging.info('saved sheet %s', sheet_name)

    ibdataloader.process_instruments(args.output_dir, product_type_codes, results_writer)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    main()
