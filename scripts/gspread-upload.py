import argparse
import csv
import json
import logging
import os
import sys

import gspread

import gservices
import ibdataloader
from gservices import update_sheet, authorize_services

_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config-gspread-upload.json'))
_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--input-dir', type=str, help='location of input directory', default='.')
    parser.add_argument('--input-prefix', type=str, help='prefix for the input files', default='ib-instr')
    parser.add_argument('--config',
                        metavar='JSON_FILENAME',
                        type=str,
                        help='location of config file, using "{}" by default'.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    help_msg_creds = 'location of Google Service Account Credentials file, using "{}" by default'
    parser.add_argument('--google-creds',
                        metavar='GOOGLE_SERVICE_ACCOUNT_CREDS_JSON',
                        type=str,
                        help=help_msg_creds.format(_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE),
                        default=_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE
                        )
    parser.add_argument('product_types', type=str, nargs='*',
                        help='download specified product types, or all if not specified')
    args = parser.parse_args()

    full_creds_path = os.path.abspath(args.google_creds)
    logging.info('reading Google Service Account credentials from "%s"', full_creds_path)
    if not os.path.isfile(full_creds_path):
        raise RuntimeError('unable to load Google Service Account credentials file: {}'.format(full_creds_path))

    full_config_path = os.path.abspath(args.config)
    logging.info('reading from config "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    config_keys = ('api_key', 'google_api_client_secret', 'token_filename', 'target_folder_id', 'db_file_prefix', 'spreadsheets')
    for config_key in config_keys:
        if config_key not in config_json.keys():
            raise RuntimeError('Key {} is missing from config file'.format(config_key))

    product_type_codes = set(args.product_types)
    if not product_type_codes.issubset(ibdataloader.get_product_type_codes()):
        allowed_types = ibdataloader.get_product_type_codes()
        logging.error('some instrument types are not defined: %s', product_type_codes.difference(allowed_types))
        sys.exit(0)

    if not product_type_codes:
        product_type_codes = ibdataloader.get_product_type_codes()

    logging.info('loading product types {}'.format(product_type_codes))

    logging.info('reading input files from %s', os.path.abspath(args.input_dir))
    os.makedirs(args.input_dir, exist_ok=True)
    available_files = [filename for filename in os.listdir(args.input_dir) if filename.startswith(args.input_prefix)]
    logging.info('files: %s', available_files)

    # saving to Google drive
    for input_file in available_files:
        _, currency, product_type_code = input_file[len(args.input_prefix):-4].split('-')
        logging.info("processing product type '%s', currency '%s'", product_type_code, currency)

        if product_type_code.lower() in config_json['spreadsheets']:
            with open(input_file) as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    print(row)

            # TODO: create currency tab if not yet existing
            # TODO: load from csv files
            spreadsheets = config_json['spreadsheets'][product_type_code.lower()]
            svc_sheet = gservices.authorize_gspread(args.google_creds)
            for spreadsheet_id in spreadsheets:
                rows = instruments
                logging.info('saving %d instruments', len(rows))
                update_sheet(svc_sheet, spreadsheet_id, header, rows)
                logging.info('saved Google sheet %s', spreadsheet_id)

        else:
            logging.info("missing key '%s' in config 'spreadsheets': not saving to Google sheet", product_type_code.lower())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
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
