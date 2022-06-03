import json
import requests
import numpy as np
import pandas as pd
import datetime
import os
import pathlib
from os.path import isfile, join
import threading
import time

# 讀取檔案裡的錢包/專案契約地址，檔案裡是放錢包地址。
opensea_totaladdress = os.path.join(os.getcwd(), 'coolcatsnft_補跑清單0513.xlsx')
input_account_addresses = pd.read_excel(opensea_totaladdress)["token_owner_address"].array


# 將檔案裡的數量分拆
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


'''
針對OpenSea API-Retrieve events 解析結構，每完成50筆會累計存成一個檔案，到最後一筆會再生成一個檔案。
抓專案契約地址 -> events_api = "https://api.opensea.io/api/v1/events?asset_contract_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
抓錢包地址 -> events_api = "https://api.opensea.io/api/v1/events?account_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
'''


def process_run(range_run, account_addresses, data_lis, api_key, event_type, thread_n, next_params):
    global data_lists
    global data_lista
    global data_listb
    global data_list0
    global data_list1

    headers = {"X-API-KEY": api_key}
    for m in range_run:

        wallet_address = account_addresses[m]
        nextpage = True
        page_num = 0

        # create a subdirectory to save response json object
        output_dir = os.path.join(os.getcwd(), 'extracts', wallet_address)
        if not os.path.isdir(os.path.join(output_dir)):
            os.makedirs(output_dir)

        if next_params:
            next_param = next_params
        else:
            next_param = ""

        try:
            while nextpage:

                events_api = "https://api.opensea.io/api/v1/events?account_address=" + wallet_address\
                             + "&event_type=" + event_type\
                             + "&cursor=" + next_param
                response = requests.get(events_api, headers=headers)
                response_json = response.json()

                with open(os.path.join(output_dir, str(page_num) + '.json'), 'w') as f:
                    json.dump(response_json, fp=f)

                if "asset_events" in response_json.keys():

                    asset_events = response_json["asset_events"]
                    for event in asset_events:

                        if event["asset"]:
                            num_sales = event["asset"]["num_sales"]
                            token_id = event["asset"]["token_id"]
                            if event["asset"]["owner"]:
                                token_owner_address = event["asset"]["owner"]["address"]
                            else:
                                token_owner_address = event["asset"]["owner"]
                        else:
                            num_sales = ""
                            token_id = ""
                            token_owner_address = ""

                        if event["asset_bundle"]:
                            asset_bundle = event["asset_bundle"]
                        else:
                            asset_bundle = ""

                        event_timestamp = event["event_timestamp"]
                        event_type = event["event_type"]

                        listing_time = event["listing_time"]

                        if event["seller"]:
                            token_seller_address = event["seller"]["address"]
                        else:
                            token_seller_address = ""

                        if event["total_price"]:
                            deal_price = int(event["total_price"])
                        else:
                            deal_price = ""

                        if event["payment_token"]:
                            payment_token_symbol = event["payment_token"]["symbol"]
                            payment_token_decimals = event["payment_token"]["decimals"]
                            payment_token_usdprice = np.float64(event["payment_token"]["usd_price"])
                        else:
                            payment_token_symbol = event["payment_token"]
                            payment_token_decimals = event["payment_token"]
                            payment_token_usdprice = event["payment_token"]

                        quantity = event["quantity"]
                        starting_price = event["starting_price"]
                        ending_price = event["ending_price"]
                        approved_account = event["approved_account"]
                        auction_type = event["auction_type"]
                        bid_amount = event["bid_amount"]

                        if event["transaction"]:
                            transaction_hash = event["transaction"]["transaction_hash"]
                            block_hash = event["transaction"]["block_hash"]
                            block_number = event["transaction"]["block_number"]
                        else:
                            transaction_hash = ""
                            block_hash = ""
                            block_number = ""

                        collection_slug = event["collection_slug"]
                        is_private = event["is_private"]
                        duration = event["duration"]
                        created_date = event["created_date"]

                        contract_address = event["contract_address"]
                        wallet_address_input = account_addresses[m]

                        data = [event_timestamp, event_type, token_id, num_sales, listing_time, token_owner_address,
                                token_seller_address, deal_price,
                                payment_token_symbol, payment_token_decimals, payment_token_usdprice, quantity,
                                starting_price, ending_price, approved_account,
                                asset_bundle, auction_type, bid_amount, transaction_hash, block_hash, block_number,
                                is_private, duration, created_date, collection_slug, contract_address,
                                wallet_address_input, page_num, "success", next_param]

                        data_lis.append(data)
                        data_lists.append(data)
                        print("wallet: " + str(m) + " , pages: " + str(page_num) + ", " + event_timestamp)

                        # for debugging
                        # if pagesnum == 2:
                        #     nextpage = False

                else:

                    wallet_address_input = account_addresses[m]

                    # pages
                    data = ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                            "", "", "", "", "", "", "", "", "", "", "", wallet_address_input, page_num,
                            "Fail-no asset_events", next_param]
                    data_lis.append(data)
                    data_lists.append(data)

                    print(str(m) + " no asset_events!")
                    nextpage = False

                if "next" in response_json.keys():
                    next_param = response_json["next"]
                    page_num += 1
                else:
                    next_param = ""
                    nextpage = False

        except:
            wallet_address_input = account_addresses[m]
            data = ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                    "", "", "", "", "", "", "", "", "", "", "", wallet_address_input, page_num, "SOMETHING WRONG",
                    next_param]

            data_lis.append(data)
            data_lists.append(data)

            if m == range_run[-1] + 1:
                return "success"
            else:
                # 記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
                rerun_range = range(m, range_run[-1] + 1)
                if (thread_n % 2) == 0:
                    data_lista.append((rerun_range, next_param))
                else:
                    data_listb.append((rerun_range, next_param))
                return "fail"

        col = ["event_timestamp", "event_type", "token_id", "num_sales", "listing_time", "token_owner_address",
               "token_seller_address", "deal_price",
               "payment_token_symbol", "payment_token_decimals", "payment_token_usdprice", "quantity", "starting_price",
               "ending_price", "approved_account",
               "asset_bundle", "auction_type", "bid_amount", "transaction_hash", "block_hash", "block_number",
               "is_private", "duration", "created_date", "collection_slug", "contract_address", "wallet_address_input",
               "pages", "msg", "next_param"]
        # 存檔，自己取名
        if (int(m) % 50 == 0 and int(m) > 0) or m == range_run[-1]:
            if (thread_n % 2) == 0:
                result_dfa = pd.DataFrame(data_lis, columns=col)
                result_dfa = result_dfa.reset_index(drop=True)
                result_dfa.to_excel(os.path.join(os.getcwd(), 'extracts', "coolcatsnft_0_" + str(m) + ".xlsx"),
                                    encoding="utf_8_sig")
            else:
                result_dfb = pd.DataFrame(data_lis, columns=col)
                result_dfb = result_dfb.reset_index(drop=True)
                result_dfb.to_excel(os.path.join(os.getcwd(), 'extracts', "coolcatsnft_1_" + str(m) + ".xlsx"),
                                    encoding="utf_8_sig")

    print("End   : " + str(datetime.datetime.now()))
    return "success"


# process_run的外層函數，當執行中斷時自動繼續往下執行
def controlfunc(process_run, range_run, addresses, data_lis, api_key, event_type, thread_n, next_param):
    global data_lista
    global data_listb
    global data_list0
    global data_list1

    s_f = process_run(range_run, addresses, data_lis, api_key, event_type, thread_n, next_param)

    rerun = True
    count = 0

    while rerun:
        if s_f == "success":
            rerun = False
            print("finished!!!!")
        else:
            if (thread_n % 2) == 0:
                if data_lista:
                    range1_rerun = data_lista[-1][0]
                    # break
                    time.sleep(60)
                    print("Rerun1 is preparing " + str(count))
                    s_f = process_run(range1_rerun, addresses, data_lis, api_key, event_type, thread_n,
                                      data_lista[-1][1])
                    count += 1
            else:
                if data_listb:
                    range2_rerun = data_listb[-1][0]
                    time.sleep(60)
                    print("Rerun2 is preparing " + str(count))
                    s_f = process_run(range2_rerun, addresses, data_lis, api_key, event_type, thread_n,
                                      data_listb[-1][1])
                    count += 1


'''

以下變數需手動設置，此程式預設調用兩個API，分別分配給兩個執行序來平行抓取處理。
event_type : 設定要抓取的事件(created, successful, cancelled, bid_entered, bid_withdrawn, transfer, offer_entered, approve)
divide : 要用多少筆數來切總列數(檔案)
range_s : 執行首序列號
range_e : 執行末序列號
EX. range(0,60) --> range_s=0 , range_e=60 , divide = 30

api_key = opensea api key1
api_key2 = opensea api key2

'''

if __name__ == '__main__':
    event_type = "successful"
    divide = 2
    range_s = 0
    range_e = 2

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

    data_lists = []
    data_lista = []
    data_listb = []
    range_collection = list(chunks(range(range_s, range_e), divide))
    thread = len(range_collection)

    start = str(datetime.datetime.now())
    for n in range(thread):
        globals()["datalist%s" % n] = []
        if (n % 2) == 0:
            globals()["add_thread%s" % n] = threading.Thread(target=controlfunc, args=(
                process_run, range_collection[n], input_account_addresses, globals()["datalist%s" % n], api_key1,
                event_type, n, ""))
            globals()["add_thread%s" % n].start()
        else:
            globals()["add_thread%s" % n] = threading.Thread(target=controlfunc, args=(
                process_run, range_collection[n], input_account_addresses, globals()["datalist%s" % n], api_key2,
                event_type, n, ""))
            globals()["add_thread%s" % n].start()

    for nn in range(thread):
        globals()["add_thread%s" % nn].join()

print("Start :" + start)
print("End   : " + str(datetime.datetime.now()))
