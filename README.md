# nimble toolkit
A collection of simple but powerful utilities for ML and Data Analytics workflows. 

# Installation
```
pip install nimble-tk
```

# Examples

All code examples, including the following, can be found in the <a href="https://github.com/sarfarazm/nimble-tk/blob/main/examples/try_nimble.ipynb" target="_blank">examples notebook</a>

## Logging

The below set of cells give some utility wrappers on python's logging functionality.
The following line will create a log file at `/tmp/try_nimble.log`
- As the logs get written, the log file will go up to max `50 MB` in size by default and then will get rolled over.
- At most, `10` such files are maintained before removing the earliest file.
- Logs will also be written to the console (in this case the notebook console) 
```python
ntk.init_file_logger(log_file_path='/tmp/try_nimble.log', console_log_on=True)
```

Output:
```
2023-12-09 11:10:43.270595 :: MainThread ::  Log file init at /tmp/try_nimble.log
```

<br/>
Example of an info log:

```python
ntk.log_info("some log message")
```

Output:
```
2023-12-09 11:10:43.289434 :: MainThread ::  some log message
```

<br/>
Example of an error log:

<br/>
Catching an exception and logging the stack trace:

```python
try:
    tmp = 1/0
except:
    ntk.log_traceback("Error while running operation")
```

Output:
```
2023-12-09 11:10:43.342127 :: MainThread ::  Error while running operation :: ZeroDivisionError: division by zero ::
 Traceback (most recent call last):
  File "/tmp/ipykernel_3709/4084614229.py", line 2, in <module>
    tmp = 1/0
ZeroDivisionError: division by zero
```

<br/>
All logs are also written to the log file:

```
! tail -10 /tmp/try_nimble.log
```

Output:
```
2023-12-09 11:10:43,268 MainThread logger.py:88:  Log file init at /tmp/try_nimble.log
2023-12-09 11:10:43,284 MainThread logger.py:88:  some log message
2023-12-09 11:10:43,311 MainThread logger.py:88:  some error message
2023-12-09 11:10:43,334 MainThread logger.py:88:  Error while running operation :: ZeroDivisionError: division by zero ::
 Traceback (most recent call last):
  File "/tmp/ipykernel_3709/4084614229.py", line 2, in <module>
    tmp = 1/0
ZeroDivisionError: division by zero
```

<br/>
Logs can also be directly written to the file <b>without logging on the console</b>. This helps in cases where we do not want to flood the console with a lot of logs.

```python
ntk.log_info_file("some log message")
ntk.log_error_file("some error message")

try:
    tmp = 1/0
except:
    ntk.log_traceback_file("Error while running operation")

! tail -7 /tmp/try_nimble.log
```

Output:

```
2023-12-09 11:10:43,501 MainThread logger.py:88:  some log message
2023-12-09 11:10:43,505 MainThread logger.py:88:  some error message
2023-12-09 11:10:43,510 MainThread logger.py:88:   :: ZeroDivisionError: division by zero ::
 Traceback (most recent call last):
  File "/tmp/ipykernel_3709/3515687902.py", line 5, in <module>
    tmp = 1/0
ZeroDivisionError: division by zero
```

## Concurrent Processing
The `ntk.run_concurrently(...)` method provides a wrapper around python's multi-processing and multi-threading functionality.

Set up a list of functions to execute:

```python
def analytic_function(customer_id):
    ntk.log_info_file(f"Running analytic_function for customer_id: {customer_id}")
    
    if customer_id == 0:
        # raising an error here to demonstrate error handling
        raise ValueError(f"Invalid customer id {customer_id}")
        
    # - Query the DB
    # - Do feature engineering
    # - Run other analytics
    
    # - Write the result data to disk or return a DF
    return pd.DataFrame({
        'CUSTOMER_ID': [customer_id]*5,
        'OTHER_DATA': np.random.randint(0, 9, 5)
    })

functions = []
for customer_id in range(10):
    functions.append((analytic_function, {'customer_id': customer_id}))
```

Run the functions concurrently with the following lines of code:

```python

results, errors = ntk.run_concurrently(functions, max_workers=ntk.get_num_cpus(), fork=True)

df_result = pd.concat([result[1] for result in results])
df_result.head(index=False)
```

`fork=True` will launch multiple processes and is recommended to be used for CPU-bound tasks.

`fork=False` will launch multiple threads, should to be used for IO-bound tasks.

<br/>
Output:

```
2023-12-09 11:10:46.767042 :: MainThread ::  Ran: 10 functions - Successfull: 9, Failed: 1
```

|   CUSTOMER_ID |   OTHER_DATA |
|---------------|--------------|
|             1 |            3 |
|             1 |            0 |
|             1 |            3 |
|             1 |            1 |
|             1 |            3 |
|             ... |            ... |


<br/><br/>
For more code examples, checkout the <a href="https://github.com/sarfarazm/nimble-tk/blob/main/examples/try_nimble.ipynb" target="_blank">examples notebook</a>
