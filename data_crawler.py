"""
針對OpenSea API-Retrieve events 解析結構，每完成50筆會累計存成一個檔案，到最後一筆會再生成一個檔案。
抓專案契約地址 -> events_api = "https://api.opensea.io/api/v1/events?asset_contract_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
抓錢包地址 -> events_api = "https://api.opensea.io/api/v1/events?account_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
"""
import json
import requests
import numpy as np
import pandas as pd
import datetime as dt
import os
from threading import Thread
import time
import s3fs


api_v1 = "https://api.opensea.io/api/v1"
test_v1 = "https://testnets-api.opensea.io/api/v1/"


def retrieve_events(api_key=None, **query_params):
    """
    OpenSea Retrieve Events wrapper

    Parameters
    ----------
    api_key : str
        an OpenSea API Key. If not defined, call testnets-api.
    query_params
        param_key=string_value, e.g. only_opensea="True"

    Returns
    -------
    Response
        a list of asset events objects and query cusors: next, previous

    Raises
    ------
    HTTPError
    """

    headers = {"X-API-KEY": api_key}

    if api_key is None:
        events_api = test_v1 + "/events"
    else:
        events_api = api_v1 + "/events"

    if query_params:
        events_api += "?"
        while query_params:
            param, value = query_params.popitem()
            events_api = events_api + param + '=' + value
            if query_params:
                events_api += "&"

    response = requests.get(events_api, headers=headers)

    if not response.ok:
        response.raise_for_status()

    return response


def parse_events(events):
    """
    Parse a dictionary representation of Response JSON object into a list of events. Each item in the list is a
    dictionary representation of an asset event.

    Parameters
    ----------
    events : a dictionary containing a collection of Event objects

    Returns
    -------
    event_list
        a list of dictionaries of each event
    """
    events_list = []

    if "asset_events" in events.keys():

        for event in events["asset_events"]:
            data = {}

            if event["asset"]:
                data["num_sales"] = event["asset"]["num_sales"]
                data["token_id"] = event["asset"]["token_id"]
                if event["asset"]["owner"]:
                    data["token_owner_address"] = event["asset"]["owner"]["address"]
                else:
                    data["token_owner_address"] = event["asset"]["owner"]

            if event["asset_bundle"]:
                data["asset_bundle"] = event["asset_bundle"]

            data["event_timestamp"] = event["event_timestamp"]
            data["event_type"] = event["event_type"]

            data["listing_time"] = event["listing_time"]

            if event["seller"]:
                data["token_seller_address"] = event["seller"]["address"]
            if event["winner_account"]:
                data["token_winner_address"] = event["winner_account"]["address"]

            if event["total_price"]:
                data["deal_price"] = int(event["total_price"])

            if event["payment_token"]:
                data["payment_token_symbol"] = event["payment_token"]["symbol"]
                data["payment_token_decimals"] = event["payment_token"]["decimals"]
                data["payment_token_usdprice"] = np.float64(event["payment_token"]["usd_price"])

            data["quantity"] = event["quantity"]
            data["starting_price"] = event["starting_price"]
            data["ending_price"] = event["ending_price"]
            data["approved_account"] = event["approved_account"]
            data["auction_type"] = event["auction_type"]
            data["bid_amount"] = event["bid_amount"]

            if event["transaction"]:
                data["transaction_hash"] = event["transaction"]["transaction_hash"]
                data["block_hash"] = event["transaction"]["block_hash"]
                data["block_number"] = event["transaction"]["block_number"]

            data["collection_slug"] = event["collection_slug"]
            data["is_private"] = event["is_private"]
            data["duration"] = event["duration"]
            data["created_date"] = event["created_date"]

            data["contract_address"] = event["contract_address"]

            data["msg"] = "success"  # @TODO: remove this; recording only error stat sufficient?
            data["next_param"] = events["next"]

            events_list.append(data)
    else:
        # @TODO: except KeyError
        data = {"msg": "Fail-no asset_events",
                "next_param": events.get("next", "")}
        events_list.append(data)

        raise KeyError("wallet: " + wallet_address + " no asset_events!")

    return events_list


def process_run(thread_n, api_key, addresses, api_param, page_num=0, data_lis=[]):
    """
    Retrieve asset events via OpenSea API based on a list of account addresses

    @TODO: not sure if thread_n is really needed here

    Parameters
    ----------
    thread_n : int

    addresses : list or array
        A user account's wallet address to filter for events on an account
        .. asset_contract_address
        .. account_address
    api-key : str

    api_param : dict
        event_type
        next_param

    page_num : int
    data_lis : list
        a list to hold dictionaries of parsed asset event elements


    Returns
    -------
    status code
        "success" or "fail/rerun"
    """
    status = "success"
    next_param = ""

    for m in range(len(addresses)):
        wallet_address = addresses[m]
        api_param.update({'account_address': wallet_address})
        # api_param.update({'asset_contract_address': wallet_address})
        try:
            next_page = True
            while next_page:
                events = retrieve_events(api_key, **api_param).json()

                # save each response JSON as a separate file
                output_dir = os.path.join(os.getcwd(), 'data', 'asset_events', wallet_address)
                # save to aws
                # output_dir = 's3://nftfomo/asset_events/' + wallet_address
                save_response_json(events, output_dir, page_num)

                e_list = parse_events(events)
                for event in e_list:
                    event["wallet_address_input"] = wallet_address
                    event["pages"] = page_num
                    data_lis.append(event)

                # @TODO: for DEBUGGING, remember to comment out or implement logging to speed up production
                print("thread: " + str(thread_n) +
                      ", wallet: " + wallet_address +
                      ", pages: " + str(page_num) +
                      ", event_timestamp: " + event["event_timestamp"])

                next_param = events["next"]
                if next_param is not None:
                    api_param['cursor'] = next_param
                    page_num += 1
                else:
                    next_param = ""
                    page_num = 0
                    next_page = False

                # @TODO: for DEBUGGING, run max 2 pages. Remember to comment or remove before production
                # if page_num == 2:
                #     next_page = False
        except requests.exceptions.RequestException as e:
            print(repr(e))
            msg = "Response [{0}]: {1}".format(e.response.status_code, e.response.reason)
            data = {"wallet_address_input": wallet_address,
                    "pages": page_num,
                    "msg": msg,
                    "next_param": next_param}
            data_lis.append(data)
            # @TODO: better workaround when 429 Client Error: Too Many Requests for url
            if e.response.status_code == 429:
                time.sleep(6)  # @TODO: make the sleep time adjustable?

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            status = ("fail/rerun", addresses[m:], next_param, page_num)
        # @TODO: remove this catch all Exception
        except Exception as e:
            print(repr(e.args))
            msg = "SOMETHING WRONG"
            data = {"wallet_address_input": wallet_address,
                    "pages": page_num,
                    "msg": msg,
                    "next_param": next_param}
            data_lis.append(data)

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            status = ("fail/rerun", addresses[m:], next_param, page_num)
        else:
            # 存檔，自己取名
            # output a file for every 50 account addresses processes or one file if less than 50 addresses total
            # m+1 because m starts at 0
            if (m+1) % 50 == 0 or (m+1) == len(addresses):
                fn = "coolcatsnft_{0}_{1}.xlsx".format(thread_n, m)
                pd.DataFrame(data_lis) \
                    .reset_index(drop=True) \
                    .to_excel(os.path.join(os.getcwd(), 'data', 'asset_events', fn), encoding="utf_8_sig")
                data_lis = []

    return status


def save_response_json(events, output_dir, page_num):
    """
    If output_dir begins with `s3://`, save the events JSON to AWS s3,
    else save to the specified local directory.

    Parameters
    ----------
    events
    output_dir
    page_num

    Returns
    -------

    """
    if output_dir.startswith('s3://'):
        output_dir.removeprefix('s3://')
        s3 = s3fs.S3FileSystem(anon=False)
        rpath = output_dir + '/' + str(page_num) + '.json'
        with s3.open(rpath, 'w') as f:
            json.dump(events, fp=f)
    else:
        # create a subdirectory to save response json object
        if not os.path.isdir(os.path.join(output_dir)):
            os.makedirs(output_dir)

        with open(os.path.join(output_dir, str(page_num) + '.json'), 'w') as f:
            json.dump(events, fp=f)


def controlfunc(func, thread_n, api_key, addresses, api_params):
    """

    Parameters
    ----------
    func
        for now, it is process_run
        @TODO different function to process for example retrieve collections
    thread_n
    addresses
    api_params : dict

    Returns
    -------

    """
    # process_run的外層函數，當執行中斷時自動繼續往下執行

    data_lis = []  # a temporary list to hold the processed values
    s_f = func(thread_n, api_key, addresses, api_params, data_lis=data_lis)

    rerun_count = 0
    rerun = True
    while rerun:
        if s_f == "success":
            rerun = False
            print("thread {} finished!!!!".format(thread_n))
        else:
            status, addresses_rerun, nxt, pg = s_f
            api_params.update({'cursor': nxt})
            rerun_count += 1
            print("Rerun {} resumes thread {}".format(rerun_count, thread_n))
            s_f = func(thread_n, api_key, addresses_rerun, api_params, pg, data_lis=data_lis)
        if rerun_count > 50:  # @TODO: parameterize this instead of hard coding
            rerun = False
            print("abort: too many errors!!!")  # @TODO: save whatever have retrieved so far


# 將檔案裡的數量分拆
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == '__main__':
    '''
    以下變數需手動設置，此程式預設調用兩個API，分別分配給兩個執行序來平行抓取處理。
    chunk_size : 要用多少筆數來切總列數(檔案)
    range_s : 執行首序列號
    range_e : 執行末序列號
    EX. range(0,60) --> range_s=0 , range_e=60 , chunk_size = 30

    api_key = opensea api key1
    api_key2 = opensea api key2
    '''
    # 讀取檔案裡的錢包/專案契約地址，檔案裡是放錢包地址。
    # @TODO: make the csv header the query parameter key
    fn = os.path.join(os.getcwd(), 'wallet_addresses.csv')
    wallet_address_inputs = pd.read_csv(fn)['account_address'].array

    chunk_size = 1
    range_s = 0
    range_e = 4
    # a list of 4 elements range(0, 4) with chunk_size of 1 will create 4 threads
    address_chunks = list(chunks(wallet_address_inputs[range_s:range_e], chunk_size))

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

    start = dt.datetime.now()
    # spawn threads based on the number of chucks
    thread_sz = len(address_chunks)
    for n in range(thread_sz):

        # distribute keys among threads
        if (n % 2) == 0:
            key_ = api_key1
        else:
            key_ = api_key2

        globals()["add_thread%s" % n] = \
            Thread(target=controlfunc,
                   args=(process_run, n, key_, address_chunks[n], {'event_type': 'successful'}))
        globals()["add_thread%s" % n].start()

    for nn in range(thread_sz):
        globals()["add_thread%s" % nn].join()

    print("Start: {}".format(start))
    print("End  : {}".format(dt.datetime.now()))
