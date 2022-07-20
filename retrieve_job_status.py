"""
Procedure to retrieve and update job status, which jobs are event-retrieving tasks
The output of the tasks are gzip json files.
The procedure reads the 2nd and the last json retrieved to update the page_num index and the cursor value
"""
import gzip
import json
import os
import pandas as pd
import s3fs


jobs_nxt = pd.read_csv(os.path.join(os.getcwd(), 'jobs.csv'))
jobs_prv = pd.read_csv(os.path.join(os.getcwd(), 'jobs_get_previous.csv'))

base_uri = 'opensea-sg/lz/asset_events/20220719/asset_contract_address/'
# base_uri = 'opensea-sg/lz/asset_events/asc-20220718T0947cst/' \
#            'asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b/'
fs = s3fs.S3FileSystem(anon=False)
ls_uri = fs.ls(base_uri)
for _uri in ls_uri:
    if fs.isdir(_uri):
        obj = [path for path in fs.ls(_uri)]
        print('number of objs:', len(obj))
        # sort objects: 1.json.gz, 10.json.gz, 2.json.gz, 3.json.gz...
        i = [int(os.path.basename(ea)[:-len('.json.gz')]) for ea in obj]
        i.sort()
        head = f'{_uri}/{i[1]}.json.gz'  # 2nd item from the head
        tail = f'{_uri}/{i[-1]}.json.gz'
        print('head:{}\ntail:{}'.format(head, tail))

        asset_contract_address = os.path.basename(_uri)
        i = jobs_nxt.asset_contract_address == asset_contract_address

        # head = get previous
        with fs.open(head, 'rb') as fread:
            with gzip.open(fread) as gz:
                events = json.load(gz)
        jobs_prv.loc[i, 'cursor'] = events['previous']

        # tail = get next
        with fs.open(tail, 'rb') as fread:
            with gzip.open(fread) as gz:
                events = json.load(gz)
        jobs_nxt.loc[i, ['cursor', 'page_num']] = [events['next'], int(os.path.basename(tail)[:-len('.json.gz')]) + 1]

    else:
        _bucket, _path, _ver_id = fs.split_path(_uri)
        print(os.path.basename(_path)[:-len('.json.gz')])

print('done')