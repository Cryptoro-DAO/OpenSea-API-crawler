"""
Retrieve asset events from a list of collection's asset contract
addresses (filter on `asset_contract_address`)

Specify `chunk_size` to divide the list into n-size chunks. Each chunk
spawns up a thread

range_s : start index of the list of address
range_e : end index of the list of addresses

api_key1, api_key2 : If a key not provided, the module calls testnets-api
"""
import os
import csv
import datetime as dt
from threading import Thread
import pandas as pd
import data_crawler as crawler


# read API keys from file
# each line in file is a key value pair separated by `=`
#   key=key_value
secrets = {}
with open(os.path.join(os.getcwd(), 'OpenSea.key')) as f:
    for line in f:
        (k, v) = line.rstrip().split('=')
        secrets[k] = v
    api_key1 = secrets['api_key1']
    api_key2 = secrets['api_key2']

jobs = pd.read_csv(os.path.join(os.getcwd(), 'jobs.csv')).to_dict('records')

chunk_size = 1
range_s = 0
range_e = 1
# a list of 4 elements range(0, 4) with chunk_size of 1 will create 4 threads
job_chunks = list(crawler.chunks(jobs[range_s:range_e], chunk_size))

output_dir = os.path.join(os.getcwd(), 'tmp')

start = dt.datetime.now()
crawler.logger.info("Start")
# spawn threads based on the number of chucks
thread_sz = len(job_chunks)
for n in range(thread_sz):
    # distribute 2 keys among threads
    if (n % 2) == 0:
        key_ = api_key1
    else:
        key_ = api_key2

    globals()["add_thread%s" % n] = Thread(target=crawler.controlfunc,
                                           args=(crawler.process_run,
                                                 key_,
                                                 job_chunks[n],
                                                 output_dir))
    globals()["add_thread%s" % n].start()

for nn in range(thread_sz):
    globals()["add_thread%s" % nn].join()

crawler.logger.info("End! Total time: {}".format(dt.datetime.now() - start))

