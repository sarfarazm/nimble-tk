import concurrent
from .. import common


def run_concurrently(function_specs: list, max_workers: int, fork: bool = True,
                     log: bool = False, logError: bool = False,
                     executor=None) -> tuple[list, list]:
    """A wrapper around python's multi-processing and multi-threading functionality.

    Args:
        function_specs (list): _description_
        max_workers (int): _description_
        fork (bool, optional): Whether to fork into multiple processes or just
                               run with multiple threads in the same process.
                               Use fork=True for CPU bound tasks.
                               Use fork=False for I/O bound tasks.
                               Defaults to True.
        log (bool, optional): _description_. Defaults to False.
        logError (bool, optional): _description_. Defaults to False.
        executor (_type_, optional): _description_. Defaults to None.

    Example usage:
    >>> import time
    >>> def analytic_function(customer_id):
    >>>     ntk.log_info_file(f"Running analytic_function for customer_id: {customer_id}")
    >>>     time.sleep(1)
    >>>     if customer_id == 0:
    >>>         # raising an error here to demonstrate error handling
    >>>         raise ValueError(f"Invalid customer id {customer_id}")
    >>>     # - Query the DB
    >>>     # - Do feature engineering
    >>>     # - Run other analytics
    >>>     # - Finally, return a DF
    >>>     return pd.DataFrame({
    >>>         'CUSTOMER_ID': [customer_id]*5,
    >>>         'OTHER_DATA': np.random.randint(0, 9, 5)
    >>>     })
    >>> 
    >>> functions = []
    >>> for customer_id in range(10):
    >>>     functions.append((analytic_function, {'customer_id': customer_id}))
    >>> 
    >>> results, errors = ntk.run_concurrently(functions, max_workers=ntk.get_num_cpus(), fork=True)
    >>> 
    >>> df_result = pd.concat([result[1] for result in results])
    >>> df_result.head().display(index=False)
    >>> 
    >>> for error in errors:
    >>>     print('Error for', error[0], ntk.exception_to_trace_string(error[1]))

    Returns:
        tuple[list, list]: Returns a tuple with two elements. 
        The first element is a list of results for successfully executed 
        functions.
        The second element is a list of exception tracebacks for failed cases.
    """
    if not executor:
        executor_fn = concurrent.futures.ThreadPoolExecutor
        if fork:
            executor_fn = concurrent.futures.ProcessPoolExecutor
        executor = executor_fn(max_workers=max_workers)

        with executor:
            return run_concurrently_with_given_executor(function_specs, log, logError, executor)
    else:
        # do not run with the 'with' clause
        return run_concurrently_with_given_executor(function_specs, log, logError, executor)


def run_concurrently_with_given_executor(function_specs, log, logError, executor):
    results, exceptions = [], []
    future_to_fn = {}
    for fn_spec in function_specs:
        fn, args = fn_spec[0], fn_spec[1]
        if type(args) == list:
            future = executor.submit(fn, *args)
        else:
            future = executor.submit(fn, **args)
        future_to_fn[future] = fn_spec

    common.log_info_file(f'{len(function_specs)} functions submitted')

    for future in concurrent.futures.as_completed(future_to_fn):
        fn_spec = future_to_fn[future]
        exception = future.exception()
        if not exception:
            result = future.result()
            results.append((fn_spec, result))
        else:
            # only log first 5 exceptions otherwise it will flood the console/log file
            fn, args = fn_spec[0], fn_spec[1]
            fn_to_string = common.map_to_string(args)
            exceptions.append((fn_to_string, exception))
            if log or logError and len(exceptions) < 5:
                common.log_error(
                    f'Error for function: {fn} {fn_to_string} - {common.exception_to_trace_string(exception)}')

    summary = f'Successfull: {len(results)}, Failed: {len(exceptions)}'
    common.log_info(f'Ran: {len(function_specs)} functions - {summary}')
    return results, exceptions
