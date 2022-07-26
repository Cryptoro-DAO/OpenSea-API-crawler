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
import pandas as pd
import data_crawler as crawler
from threading import Thread


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

jobs = pd.read_csv(os.path.join(os.getcwd(), 'data', 'jobs_get_next.csv'), index_col=0)
jobs = jobs[jobs.n_request > 0]
jobs['collection_slug'] = [os.path.basename(url) for url in jobs.collection_url]
jobs.drop(columns='asset_contract_address', inplace=True)
jobs = jobs.to_dict('records')

chunk_size = 24
range_s = 0
range_e = 140
# a list of 140 elements range(0, 140) with chunk_size of 24 will create 6 threads
job_chunks = list(crawler.chunks(jobs[range_s:range_e], chunk_size))

output_dir = os.path.join(os.getcwd(), 'tmp')
# output_dir = 's3://opensea-sg/lz/asset_events/20220727/'

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
