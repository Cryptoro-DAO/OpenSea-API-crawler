import os
import re
import json


data_dir = os.path.join(os.getcwd(), 'data', 'asset_events', 'asset_contract_address')
# get full paths to directories; excluding files
addr_dir = (os.path.join(data_dir, each_subdir) for each_subdir in os.listdir(data_dir))
addr_dir = (e for e in addr_dir if os.path.isdir(e))

# get only .json from each directory, sort the file numerically, and
# combine them into one multi-line .json
for each_dir in addr_dir:
    ix = [int(each_json[:-5]) for each_json in os.listdir(each_dir) if re.search('\.json', each_json)]
    ix.sort()
    fn = (os.path.join(each_dir, f'{i}.json') for i in ix)

    asset_events = []
    for each_json in fn:
        with open(each_json, 'r') as f:
            resp = json.load(f)
            asset_events.extend(resp['asset_events'])
    with open(os.path.join(data_dir, os.path.basename(each_dir) + '.json'), 'w') as f:
        json.dump(asset_events, fp=f)

    print(len(asset_events))
