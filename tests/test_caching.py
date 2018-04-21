import logging
import random
import unittest
from datetime import datetime
from datetime import timedelta

from webscrapetools.taskpool import TaskPool
from webscrapetools.urlcaching import set_cache_path, empty_cache, read_cached, invalidate_expired_entries, is_cached


class TestStringMethods(unittest.TestCase):

    def setUp(self):
        pass

    def test_random_access_multithreaded(self):
        set_cache_path('../output/tests', max_node_files=400, rebalancing_limit=1000)
        empty_cache()
        tasks = TaskPool(30)

        def open_test_random(key):

            def inner_open_test_random(inner_key):
                return 'content for key %s: %s' % (inner_key, random.randint(1, 100000))

            content = read_cached(inner_open_test_random, key)
            return content

        for count in range(10000):
                tasks.add_task(open_test_random, count)

        results = tasks.execute()
        logging.info('results: %s', results)
        empty_cache()

    def test_expiration(self):
        test_output_dir = '../output/tests'
        set_cache_path(test_output_dir, max_node_files=400, rebalancing_limit=1000, expiry_days=3)
        empty_cache()

        def read_random_value(key):
            return 'content for key %s: %s' % (key, random.randint(1, 100000))

        read_cached(read_random_value, key='abc')
        read_cached(read_random_value, key='def')
        read_cached(read_random_value, key='ghf')

        self.assertTrue(is_cached('abc'))
        self.assertTrue(is_cached('def'))
        self.assertTrue(is_cached('ghf'))

        future_date = datetime.today() + timedelta(days=10)
        invalidate_expired_entries(as_of_date=future_date)
        self.assertFalse(is_cached('abc'))
        self.assertFalse(is_cached('def'))
        self.assertFalse(is_cached('ghf'))

        empty_cache()

    def tearDown(self):
        empty_cache()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()