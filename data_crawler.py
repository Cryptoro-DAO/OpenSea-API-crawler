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
import time
import s3fs
import logging
from threading import Thread


# create logger with 'spam_application'
logger = logging.getLogger('data_crawler')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('log\crawler.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


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

        raise KeyError("no asset_events!")

    return events_list


def process_run(thread_n, api_key, api_params, page_num=0, data_lis=None):
    """
    Retrieve asset events via OpenSea API based on a list of account addresses

    @TODO: not sure if thread_n is really needed here

    Parameters
    ----------
    thread_n : int
        index to track the thread number
    api_key : str
        OpenSea API key, if None or '', testnets-api is used
    api_params : dict
        query parameters:
            event_type
            cursor
            account_address : list
            asset_contract_address : list
        * currently supports only one list at a time, do not specify both account_address and asset_contract_address
    page_num : int
        index to track the number of event pages, see cursor
    data_lis : list
        a list to hold dictionaries of parsed asset event elements

    Returns
    -------
    status code
        "success" or "fail/rerun"
    """
    if data_lis is None:
        data_lis = []
    status = "success"
    next_param = ""

    # check type of addresses {'account_address', 'asset_contract_address'}
    for param, value in api_params.items():
        if param in ['account_address', 'asset_contract_address']:
            address_filter = param
            addresses = value

    data_dir = os.path.join(os.getcwd(), 'data', 'asset_events', address_filter)
    for m in range(len(addresses)):
        _address = addresses[m]
        api_params.update({address_filter: _address})

        try:
            next_page = True
            while next_page:
                events = retrieve_events(api_key, **api_params).json()

                # save each response JSON as a separate file
                output_dir = os.path.join(data_dir, _address)
                # save to aws
                # output_dir = 's3://nftfomo/asset_events/' + _address
                save_response_json(events, output_dir, page_num)

                e_list = parse_events(events)
                for event in e_list:
                    event[address_filter + '_input'] = _address
                    event['pages'] = page_num
                    data_lis.append(event)

                logger.debug('thread: {}, {}: {}, page: {}, event_timestamp: {}'
                            .format(thread_n, address_filter, _address, page_num, event['event_timestamp']))

                next_param = events['next']
                if next_param is not None:
                    api_params['cursor'] = next_param
                    page_num += 1
                else:
                    api_params.pop('cursor')
                    next_param = ''
                    page_num = 0
                    next_page = False

                # @TODO: for DEBUGGING, run max 2 pages. Remember to comment or remove before production
                # if page_num == 2:
                #     api_params.pop('cursor')
                #     next_param = ''
                #     page_num = 0
                #     next_page = False
        except requests.exceptions.RequestException as e:
            # @TODO: better workaround when 429 Client Error: Too Many Requests for url
            # @TODO: 524 Server Error << Cloudflare Timeout?
            logger.error(repr(e))
            msg = f"Response [{e.response.status_code}]: {e.response.reason}"
            data = {address_filter + "_input": _address,
                    "pages": page_num,
                    "msg": msg,
                    "next_param": next_param}
            data_lis.append(data)
            if e.response.status_code == 429:
                time.sleep(6)  # @TODO: make the sleep time adjustable?

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            api_params.update({address_filter: addresses[m:], 'cursor': next_param})
            status = ("fail/rerun", api_params, page_num)
        # @TODO: remove this catch all Exception
        except Exception as e:
            logger.error(repr(e.args))
            msg = "SOMETHING WRONG"
            data = {address_filter + "_input": _address,
                    "pages": page_num,
                    "msg": msg,
                    "next_param": next_param}
            data_lis.append(data)

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            api_params.update({address_filter: addresses[m:], 'cursor': next_param})
            status = ("fail/rerun", api_params, page_num)
        else:
            # 存檔，自己取名
            # output a file for every 50 account addresses processes or one file if less than 50 addresses total
            # m+1 because m starts at 0
            if (m+1) % 50 == 0 or (m+1) == len(addresses):
                fn = "{}_{}_{}.xlsx".format(address_filter, thread_n, m)
                pd.DataFrame(data_lis) \
                    .reset_index(drop=True) \
                    .to_excel(os.path.join(data_dir, fn), encoding="utf_8_sig")
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


def controlfunc(func, thread_n, api_key, api_params):
    """
    process_run的外層函數，當執行中斷時自動繼續往下執行

    Parameters
    ----------
    func
        for now, it is process_run
        @TODO different function to process for example retrieve collections
    thread_n
    api_key
    api_params : dict

    Returns
    -------

    """

    data_lis = []  # a temporary list to hold the processed values

    rerun_count = 0
    page_num = 0
    rerun = True
    while rerun:
        s_f = func(thread_n, api_key, api_params, page_num, data_lis=data_lis)
        if s_f == "success":
            rerun = False
            logger.info(f'thread {thread_n} finished!!!!')
        else:
            status, api_params, page_num = s_f
            rerun_count += 1
            logger.info('Rerun {} resumes thread {}'.format(rerun_count, thread_n))
        if rerun_count > 50:  # @TODO: parameterize this instead of hard coding
            rerun = False
            logger.critical(f'Thread {thread_n} abort: too many errors!!!')  # @TODO: save whatever have retrieved so far


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

    api_key1 = opensea api key1
    api_key2 = opensea api key2
    '''
    # 讀取檔案裡的錢包/專案契約地址，檔案裡是放錢包地址。
    # @TODO: make the csv header the query parameter key
    # fn = os.path.join(os.getcwd(), 'wallet_addresses.csv')
    # address_inputs = pd.read_csv(fn)['account_address'].values
    fn = os.path.join(os.getcwd(), 'NFT_20_list.csv')
    address_inputs = pd.read_csv(fn)['collection_address'].values

    chunk_size = 7
    range_s = 0
    range_e = 21
    # a list of 4 elements range(0, 4) with chunk_size of 1 will create 4 threads
    address_chunks = list(chunks(address_inputs[range_s:range_e], chunk_size))

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

    logger.info("Start")
    # spawn threads based on the number of chucks
    thread_sz = len(address_chunks)
    for n in range(thread_sz):
        # distribute keys among threads
        if (n % 2) == 0:
            key_ = api_key1
        else:
            key_ = api_key2

        # filter_params = {'account_address': address_chunks[n], 'event_type': 'successful', 'limit': '100'}
        # filter_params = {'asset_contract_address': address_chunks[n], 'event_type': 'successful', 'limit': '100'}
        filter_params = {'asset_contract_address': address_chunks[n], 'limit': '100'}
        globals()["add_thread%s" % n] = \
            Thread(target=controlfunc,
                   args=(process_run, n, key_, filter_params))
        globals()["add_thread%s" % n].start()

    for nn in range(thread_sz):
        globals()["add_thread%s" % nn].join()

    logger.info("End")
