import os
import re
import json


data_dir = os.path.join(os.getcwd(), 'data', 'asset_events', 'asset_contract_address')
# get full paths to directories; excluding files
addr_dir = (os.path.join(data_dir, each_subdir) for each_subdir in os.listdir(data_dir))
addr_dir = (e for e in addr_dir if os.path.isdir(e))

# get only .json from each directory, sort the file numerically, and
# combine them into one single-line .json
for each_dir in addr_dir:
    ix = sorted([int(each_json[:-5]) for each_json in os.listdir(each_dir) if re.search('\.json', each_json)])
    fn = (os.path.join(each_dir, f'{i}.json') for i in ix)

    i = 0
    with open(os.path.join(data_dir, os.path.basename(each_dir) + '.json'), 'w', encoding='utf8') as fw:
        for each_json in fn:
            with open(each_json, 'r') as fr:
                resp = json.load(fr)
            for e in resp['asset_events']:
                fw.write(str(e) + '\n')
                i += 1

    print(f'{i}, {os.path.basename(each_dir)}')
