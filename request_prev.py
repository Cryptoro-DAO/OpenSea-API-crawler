import os
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

jobs = pd.read_csv(os.path.join(os.getcwd(), 'jobs.csv'))

filter_params = {'asset_contract_address': jobs.asset_contract_address, 'cursor': jobs.previous[0], 'limit': jobs.limit[0].astype('str')}

# crawler.process_run(api_key1, filter_params, page_num=jobs.page_num[0], n_request=2615, ascending=True)

# output_dir = os.path.join(os.getcwd(), 'data', 'v')
# crawler.controlfunc(crawler.process_run, api_key1, filter_params,
#                     page_num=jobs.page_num[0], n_request=2615, ascending=True,
#                     output_dir=output_dir)
output_dir = os.path.join(os.getcwd(), 'tmp')
crawler.controlfunc(crawler.process_run, api_key1, filter_params, output_dir=output_dir, ascending=True)
