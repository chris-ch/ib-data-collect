import logging
from multiprocessing.pool import ThreadPool


class TaskPool(object):
    """
    Running a pool of tasks on a limited number of threads.
    """

    def __init__(self, pool_size=5):
        self._pool_size = pool_size
        self._pool = ThreadPool(pool_size)
        self._tasks_args = list()

    @staticmethod
    def _task_function_wrapper(single_param):
        wrapped_task, wrapped_task_id, wrapped_args, wrapped_kwargs = single_param
        try:
            result = wrapped_task(*wrapped_args, **wrapped_kwargs)

        except Exception as err:
            logging.error('task %d (%s, %s, %s) failed: %s',
                          wrapped_task_id, wrapped_task, wrapped_args, wrapped_kwargs,
                          err, exc_info=True)
            logging.warning('retrying failed task %d', wrapped_task_id)
            try:
                result = wrapped_task(*wrapped_args, **wrapped_kwargs)

            except Exception as err_2:
                logging.error('task %d (%s, %s, %s) failed twice: %s',
                              wrapped_task_id, wrapped_task, wrapped_args, wrapped_kwargs,
                              err_2, exc_info=True)
                raise

        return result

    def add_task(self, task_function, *args, **kwargs):
        """
        Adding a new task to the pool.
        :param task_function: function to be run for the task
        :param args: positional arguments to be passed on to the task function
        :param kwargs: keyword arguments to be passed on to the task function
        :return:
        """
        task_id = len(self._tasks_args) + 1
        self._tasks_args.append((task_function, task_id, args, kwargs))

    def execute(self):
        """
        Starts executing the tasks and wait for their completion.
        :return:
        """
        logging.info('processing %d tasks on a pool size of %d', len(self._tasks_args), self._pool_size)
        if self._pool_size == 1:
            results = list()
            for task_args in self._tasks_args:
                result = TaskPool._task_function_wrapper(task_args)
                results.append(result)

        else:
            results = self._pool.map(TaskPool._task_function_wrapper, self._tasks_args)

        self._pool.close()
        self._pool.join()

        # serialized version below
        #results = list()
        #for task_function, task_id, args, kwargs in self._tasks_args:
        #    result = task_function(*args, **kwargs)
        #    results.append(result)

        return results
