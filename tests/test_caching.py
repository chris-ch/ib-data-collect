import logging
import random
import unittest

from taskpool import TaskPool
from urlcaching import set_cache_path, read_cached, empty_cache


class TestStringMethods(unittest.TestCase):

    def setUp(self):
        set_cache_path('../output/tests', max_node_files=400, rebalancing_limit=1000)
        empty_cache()

    def test_random_access_multithreaded(self):
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

    def tearDown(self):
        empty_cache()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()