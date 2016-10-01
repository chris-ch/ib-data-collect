import argparse
import json
import os
import logging

from gservices import setup_services, prepare_sheet
from oauth2client import tools

_IBROKERS_DB_FOLDER_ID = '0B4QNJgt5Fd0fQ1M0ckVBWVpJdkk'
_IBROKERS_DB_FILENAME = 'IBrokers Instruments DB'
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     parents=[tools.argparser]
                                     )
    parser.add_argument('--config',
                        metavar='JSON_FILENAME',
                        type=str,
                        help='location of config file, using "{}" by default'.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )

    args = parser.parse_args()
    full_config_path = os.path.abspath(args.config)
    logging.info('reading from default config "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    config_keys = ('api_key', 'google_api_client_secret', 'token_filename')
    for config_key in config_keys:
        if config_key not in config_json.keys():
            raise RuntimeError('Key {} is missing from config file'.format(config_key))

    client_secret_filename = config_json['google_api_client_secret']
    api_key = config_json['api_key']
    token_filename = config_json['token_filename']
    drive, sheets = setup_services(client_secret_filename, api_key, token_filename, args)
    print(prepare_sheet(drive, sheets, _IBROKERS_DB_FOLDER_ID, _IBROKERS_DB_FILENAME))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    main()
