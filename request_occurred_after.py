"""
Retrieve asset events occurred before certain date

api_key1, api_key2 : If a key not provided, the module calls testnets-api
"""
import os
import datetime as dt
import numpy as np
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

job = dict(occurred_after='2022-07-01T00:00', limit=300, n_request=5)

output_dir = os.path.join(os.getcwd(), 'tmp', 'asset_events', '20220701')

start = dt.datetime.now()
crawler.logger.info("Start")
crawler.process_run(api_key2, [job], output_dir)
crawler.logger.info("End! Total time: {}".format(dt.datetime.now() - start))
