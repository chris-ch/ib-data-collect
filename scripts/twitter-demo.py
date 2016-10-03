import argparse
import json
import logging
import os

import tweepy

_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
    config_keys = ('api_key', 'google_api_client_secret', 'token_filename',
                   'target_folder_id', 'db_file_prefix',
                   'twitter_consumer_token', 'twitter_consumer_secret',)
    for config_key in config_keys:
        if config_key not in config_json.keys():
            raise RuntimeError('Key {} is missing from config file'.format(config_key))


    consumer_token = args.consumer_token
    consumer_secret = args.consumer_secret
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    main()
