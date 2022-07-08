"""
OpenSea API-Retrieve events
    Parse events, save the response to local disk or s3
    Make use of API's cursor to retrieve previous or next page
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
fh = logging.FileHandler('log/crawler.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(threadName)s: %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

api_v1 = "https://api.opensea.io/api/v1"
test_v1 = "https://testnets-api.opensea.io/api/v1/"


class process_run_err(Exception):
    """
    Exception raised for errors in process_run
    @TODO

    """

    def __init__(self, a, message="..."):
        self.a = a
        self.message = message
        super().__init__(self.message)


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
        a list of asset events objects and query cursors: next, previous

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
            data["next_param"] = events["next"]  # @TODO: remove this; probably not needed as part of the dataframe

            events_list.append(data)
    else:
        # @TODO: except KeyError
        data = {"msg": "Fail-no asset_events",
                "next_param": events.get("next", "")}
        events_list.append(data)

        raise KeyError("no asset_events!")

    return events_list


def process_run(api_key, api_params, page_num=1, n_request=1, ascending=False, output_dir=None):
    """
    Retrieve asset events via OpenSea API based on a list of account addresses
    @TODO: extend this method to page forward or backward via cursor param

    Parameters
    ----------
    output_dir : str
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
    n_request : int
        number requests to make
    ascending : bool


    Returns
    -------
    status code
        "success", or if failed a tuple (message, current API parameters, current page count)
    """
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), 'data')
    status = "success"
    _cursor = ""

    # check type of addresses {'account_address', 'asset_contract_address'}
    for param, value in api_params.items():
        if param in ['account_address', 'asset_contract_address']:
            address_filter = param
            addresses = value

    for m, _address in enumerate(addresses):
        api_params.update({address_filter: _address})

        try:
            next_page = n_request
            while next_page:
                events = retrieve_events(api_key, **api_params).json()

                # save each response JSON as a separate file
                if output_dir.startswith('s3://'):
                    # save to aws
                    # _dir = 's3://nftfomo/asset_events/' + _address
                    _dir = f'{output_dir}/{address_filter}/{_address}'
                else:
                    _dir = os.path.join(output_dir, address_filter, _address)
                save_response_json(events, _dir, page_num)
                logger.debug(f'saved {address_filter}: {_address}, page: {page_num}')

                if ascending:
                    _cursor = events['previous']
                else:
                    _cursor = events['next']
                if _cursor is not None:
                    api_params['cursor'] = _cursor
                    next_page -= 1
                    if ascending:
                        page_num -= 1
                    else:
                        page_num += 1
                else:
                    logger.info(f'{_address} finished: {page_num} page(s)')
                    api_params.pop('cursor')
                    _cursor = ''
                    next_page = False

        except requests.exceptions.HTTPError as err:
            # @TODO: better workaround when 429 Client Error: Too Many Requests for url
            # @TODO: 520 Server Error
            # @TODO: 524 Server Error << Cloudflare Timeout?
            logger.error(repr(err))

            if err.response.status_code == 429:
                time.sleep(6)  # @TODO: make the sleep time adjustable?

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            api_params.update({address_filter: addresses[m:], 'cursor': _cursor})
            status = (addresses[m], api_params, page_num)
        # except requests.exceptions.SSLError
        # @TODO: requests.exceptions.SSLError:
        #   HTTPSConnectionPool(host='api.opensea.io', port=443):
        #   Max retries exceeded with url
        except requests.exceptions.RequestException as e:
            logger.error(repr(e))

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            api_params.update({address_filter: addresses[m:], 'cursor': _cursor})
            status = (addresses[m], api_params, page_num, next_page)
        # @TODO: remove this catch all Exception
        except Exception as e:
            logger.error(repr(e.args))

            # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
            api_params.update({address_filter: addresses[m:], 'cursor': _cursor})
            status = (addresses[m], api_params, page_num, next_page)

    return status


def events_json_to_dataframe(json_path):
    """
    Parse OpenSea Event objects into a pandas DataFrame

    Parameters
    ----------
    json_path
        absolute path

    Returns
    -------

    """
    lst = []
    if not isinstance(json_path, list):
        json_path = [json_path]
    for p in json_path:
        with open(p) as f:
            data = json.load(f)
        lst.extend(parse_events(data))

    return pd.DataFrame(lst)


def df_to_parquet(df: pd.DataFrame, out_path):
    """
    minimal imputing missing values and conversion of dtype prior to saving to parquet

    Parameters
    ----------
    df
    out_path

    Returns
    -------

    """
    datetime_col = ['event_timestamp', 'listing_time', 'created_date']
    df.loc[:, datetime_col] = df.loc[:, datetime_col].apply(pd.to_datetime)
    num_col = ['num_sales', 'quantity', 'deal_price', 'starting_price', 'ending_price']
    df.loc[:, num_col] = df.loc[:, num_col].fillna(0).apply(pd.to_numeric, errors='coerce', downcast='integer')
    df.to_parquet(out_path)


def to_excel(address_filter, addresses, data_dir, data_lis, m):
    """

    Parameters
    ----------
    address_filter
    addresses
    data_dir
    data_lis
        a list holding Event's parsed elements
    m

    Returns
    -------

    """
    # @TODO: this doesn't scale well; holding too much data in memory and the Excel gets too big and slow to read
    # 存檔，自己取名
    # output a file for every 50 account addresses processes or one file if less than 50 addresses total
    # m+1 because m starts at 0
    if (m + 1) % 50 == 0 or (m + 1) == len(addresses):
        fn = "{}_{}.xlsx".format(address_filter, m)
        pd.DataFrame(data_lis) \
            .reset_index(drop=True) \
            .to_excel(os.path.join(data_dir, fn), encoding="utf_8_sig")
        data_lis = []
    return data_lis


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
        s3 = s3fs.S3FileSystem(anon=False)
        rpath = output_dir[5:] + '/' + str(page_num) + '.json'
        with s3.open(rpath, 'w') as fwrite:
            json.dump(events, fp=fwrite)
    else:
        # create a subdirectory to save response json object
        if not os.path.isdir(os.path.join(output_dir)):
            os.makedirs(output_dir)
        with open(os.path.join(output_dir, str(page_num) + '.json'), 'w') as fwrite:
            json.dump(events, fp=fwrite)


def controlfunc(func, api_key, api_params, retry_max=10,
                page_num=1,
                n_request=1,
                ascending=False,
                output_dir=None):
    """
    process_run的外層函數，當執行中斷時自動繼續往下執行

    Parameters
    ----------
    func
        for now, it is process_run
        @TODO different function to process for example retrieve collections
    api_key
    api_params : dict
        see example
    retry_max : int
        maximum number of retries when encounters exception
    n_request : int, default 1
    page_num : int, default 1
    ascending : bool, default False
    output_dir
    """
    retry_count = 0
    run = True
    while run:
        s_f = func(api_key, api_params, page_num, n_request, ascending, output_dir)
        if s_f == "success":
            if retry_count > 0:
                retry_count -= 1
            run = False
            logger.info('finished!!!!')
        else:
            msg, api_params, page_num, n_request = s_f
            retry_count += 1
            logger.info(f"Retry {retry_count}: {msg}")
        if retry_count > retry_max:
            run = False
            logger.critical('Abort!!! Too many errors!!!')


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == '__main__':
    """
    Example to retrieve asset events from a list of user account's wallet
    addresses (filter on `account_address`)

    Specify `chunk_size` to divide the list into n-size chunks. Each chunk
    spawns up a thread
    
    range_s : start index of the list of address
    range_e : end index of the list of addresses

    api_key1, api_key2 : If a key not provided, the module calls testnets-api
    """
    # Read a list of wallet addresses from file
    # @TODO: make the csv header the query parameter key
    fn = os.path.join(os.getcwd(), 'wallet_addresses.csv')
    address_inputs = pd.read_csv(fn)['account_address'].values
    # Read a list of collection contract addresses from file
    # fn = os.path.join(os.getcwd(), 'NFT_20_list.csv')
    # address_inputs = pd.read_csv(fn)['collection_address'].values

    chunk_size = 1
    range_s = 0
    range_e = 4
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

    start = dt.datetime.now()
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
        filter_params = {'account_address': address_chunks[n], 'limit': '100'}
        # filter_params = {'asset_contract_address': address_chunks[n], 'event_type': 'successful', 'limit': '100'}
        # filter_params = {'asset_contract_address': address_chunks[n], 'limit': '100'}
        globals()["add_thread%s" % n] = Thread(target=controlfunc, args=(process_run, key_, filter_params))
        globals()["add_thread%s" % n].start()

    for nn in range(thread_sz):
        globals()["add_thread%s" % nn].join()

    logger.info("End! Total time: {}".format(dt.datetime.now() - start))
