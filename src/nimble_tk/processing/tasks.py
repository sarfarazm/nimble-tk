import concurrent
import common


def run_in_parallel(function_specs, max_workers, fork=True, log=False, logError=False,
                    log_interval=None, executor=None):
    """
    Run functions in parallel:

    Sample code:

    proxies = { "http"  : '', "https" : ''}
    fn_specs = []
    for row in df.iterrows():
        fn_specs.append((fn_specs, {'row':row}))
    results, exception_tuples = run_in_parallel(fn_specs, get_num_cpus(), fork=True, log=False)
    ret_vals = [result for fn_spec, result in results]
    for exception in exception_tuples:
        exception_to_trace_string(exception[1])
    """
    if not executor:
        executor_fn = concurrent.futures.ThreadPoolExecutor
        if fork:
            executor_fn = concurrent.futures.ProcessPoolExecutor
        executor = executor_fn(max_workers=max_workers)

        with executor:
            return run_in_parallel_with_given_executor(function_specs, log, logError, log_interval, executor)
    else:
        # do not run with the 'with' clause
        return run_in_parallel_with_given_executor(function_specs, log, logError, log_interval, executor)


def run_in_parallel_with_given_executor(function_specs, log, logError, log_interval, executor):
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

    if not log_interval:
        log_interval = len(function_specs) / 10
        log_interval = int(log_interval)
        if log_interval <= 1:
            log_interval = 1
    stopwatch = common.StopWatch()
    for future in concurrent.futures.as_completed(future_to_fn):
        fn_spec = future_to_fn[future]
        exception = future.exception()
        if not exception:
            result = future.result()
            results.append((fn_spec, result))
            if (len(results) % log_interval) == 0:
                pct_done = round(len(results)*100.0 / len(function_specs))
                common.log_info_file(
                    f'{len(results)} functions done out of {len(function_specs)} ({pct_done}%) - Time taken: {stopwatch.get_time_hhmmss()} - , errors: {len(exceptions)}')
        else:
            # only log first 5 exceptions otherwise it will flood the console/log file
            fn, args = fn_spec[0], fn_spec[1]
            fn_to_string = common.map_to_string(args)
            exceptions.append((fn_to_string, exception))
            if log or logError and len(exceptions) < 5:
                common.log_error(
                    f'Error for function: {fn} {fn_to_string} - {common.exception_to_trace_string(exception)}')

    return results, exceptions
