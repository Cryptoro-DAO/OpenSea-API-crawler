"""
Combines a list of JSON-like events dict into a parquet
"""
import os
import re
import pandas as pd
import data_crawler as crawler

data_dir = os.path.join(os.getcwd(), 'data', 'asset_events', 'account_address')
fpath = (os.path.join(data_dir, x) for x in os.listdir(data_dir))

for acc_addr_dir in fpath:
    if os.path.isdir(acc_addr_dir):
        j_path = [os.path.join(acc_addr_dir, _json) for _json in os.listdir(acc_addr_dir) if re.search('\.json', _json)]
        df = crawler.events_json_to_dataframe(j_path)
        crawler.df_to_parquet(df, os.path.join(data_dir, os.path.basename(acc_addr_dir) + '.parquet'))

