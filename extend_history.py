"""
Extends event history by making use of the cursors (previous or next) on either end of transaction history
"""
import os
import re
import json


data_dir = os.path.join(os.getcwd(), 'data', 'asset_events', 't')
# get full paths to directories; excluding files
addr_dir = (os.path.join(data_dir, each_subdir) for each_subdir in os.listdir(data_dir))
addr_dir = (e for e in addr_dir if os.path.isdir(e))


if __name__ == '__main__':
    # get only .json from each directory, sort the file numerically, and
    # combine them into one single-line .json
    lst = []
    for each_dir in addr_dir:
        ix = sorted([int(each_json[:-5]) for each_json in os.listdir(each_dir) if re.search('\.json', each_json)])
        fn = [os.path.join(each_dir, f'{i}.json') for i in ix]

        # with open(fn[-1], 'r') as fr:
        #     # tail must have a previous, if not go to the one before
        #     tail_1 = json.load(fr)
        #     prv, nxt = retrieve_cursors(tail_1)
        #     print('{}, previous{}, next:{}'.format('tail_1', prv, nxt))
        # with open(fn[0], 'r') as fr:
        #     head = json.load(fr)
        #     print('{},previous:{},next:{}'.format('head', head['previous'], head['next']))

        # checking for cursor anomaly: should only be 1 previous == None and 1 next == None if the list is complete
        for each_fn in fn:
            with open(each_fn, 'r') as fr:
                events = json.load(fr)
                prv, nxt = events['previous'], events['next']
                lst.append({'previous': prv, 'next': nxt,
                            'fn': os.path.basename(each_fn), 'addr': os.path.basename(each_dir)})

