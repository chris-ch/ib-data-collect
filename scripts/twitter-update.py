import argparse
from datetime import datetime
import json
import logging
import os

import sys
from twitter import OAuth
from twitter import Twitter
from twitter import oauth_dance
from twitter import read_token_file

_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def main():
    parser = argparse.ArgumentParser(description='Publishing IBroker DB updates on twitter',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     )
    parser.add_argument('--config',
                        metavar='JSON_FILENAME',
                        type=str,
                        help='location of config file, using "{}" by default'.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    parser.add_argument('--log-only', action='store_true', help='only logs message to be published, no twitter update')
    args = parser.parse_args()
    full_config_path = os.path.abspath(args.config)
    logging.info('reading from config "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    config_keys = ('target_folder_id', 'twitter_consumer_token', 'twitter_consumer_secret', 'twitter_token_filename')
    for config_key in config_keys:
        if config_key not in config_json.keys():
            raise RuntimeError('Key {} is missing from config file'.format(config_key))

    consumer_token = config_json['twitter_consumer_token']
    consumer_secret = config_json['twitter_consumer_secret']
    twitter_token_filename = config_json['twitter_token_filename']

    if not os.path.isfile(twitter_token_filename):
        oauth_dance('announcements-app', consumer_token, consumer_secret, twitter_token_filename)

    oauth_token, oauth_token_secret = read_token_file(twitter_token_filename)

    twitter_service = Twitter(auth=OAuth(oauth_token, oauth_token_secret, consumer_token, consumer_secret))
    target_folder_id = config_json['target_folder_id']
    publish_twitter(twitter_service, target_folder_id, args.log_only)


def publish_twitter(twitter_service, target_folder_id, log_only=False):
    message = '({0} update) #InteractiveBroker mapping conId vs available instruments (stocks, ETFs, future contracts): {1}'
    dir_url = 'https://drive.google.com/open?id={}'.format(target_folder_id)
    month = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')[datetime.today().month - 1]
    twitter_message = message.format(month, dir_url)
    logging.info('publishing message: %s', twitter_message)
    if not log_only:
        timeline = twitter_service.statuses.home_timeline()[0]
        logging.info('{}: {}'.format(timeline['user'], timeline['text']))
        twitter_service.statuses.update(status=twitter_message)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logname = os.path.abspath(sys.argv[0]).split(os.sep)[-1].split(".")[0]
    file_handler = logging.FileHandler(logname + '.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    main()
