"""
Procedure to read and update job status, where jobs are event-retrieving tasks
The output of the tasks are gzip json files.
The procedure reads the 2nd and the last file retrieved to update the page_num index and the cursor value
"""
import gzip
import json
import os
import pandas as pd
import s3fs


jobs = pd.read_csv(os.path.join(os.getcwd(), 'jobs.csv'))

base_uri = 'opensea-sg/lz/asset_events/20220719/asset_contract_address/'
# base_uri = 'opensea-sg/lz/asset_events/asc-20220718T0947cst/' \
#            'asset_contract_address/0x23581767a106ae21c074b2276D25e5C3e136a68b/'
fs = s3fs.S3FileSystem(anon=False)
asset_contract_addr = fs.ls(base_uri)
for _addr in asset_contract_addr:
    if fs.isdir(_addr):
        uri = [path for path in fs.ls(_addr)]
        print(len(uri))
        # sort URI
        i = [int(os.path.basename(ea)[:-len('.json.gz')]) for ea in uri]
        i.sort()
        head_2 = f'{_addr}/{i[1]}.json.gz'  # 2nd item from the head
        tail = f'{_addr}/{i[-1]}.json.gz'
        print(head_2, tail)

        with fs.open(head_2, 'rb') as fread:
            with gzip.open(fread) as gz:
                events = json.load(gz)
                print(os.path.basename(head_2), events['next'], events['previous'])

    else:
        _bucket, _path, _ver_id = fs.split_path(_addr)
        print(os.path.basename(_path)[:-len('.json.gz')])
