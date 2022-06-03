import json
import requests
import numpy as np
import pandas as pd
import datetime
import os
import pathlib
import xlwings as xw
from os.path import isfile,join
import threading
import time

#讀取檔案裡的錢包/專案契約地址，檔案裡是放錢包地址。
opensea_totaladdress = "C:\\Users\\Fred\\Desktop\\opensea_wallet\\coolcatsnft_補跑清單0513.xlsx"
xw.Book(opensea_totaladdress).sheets[0].activate()
df_opensea_totaladdress = xw.Range(xw.Range("A1").expand().address).options(pd.DataFrame,index=False).value

#將檔案裡的數量分拆
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

'''
針對OpenSea API-Retrieve events 解析結構，每完成50筆會累計存成一個檔案，到最後一筆會再生成一個檔案。
抓專案契約地址 -> events_api = "https://api.opensea.io/api/v1/events?asset_contract_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
抓錢包地址 -> events_api = "https://api.opensea.io/api/v1/events?account_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
'''
def process_run(range_run,df_opensea_totaladdress,data_lis,api_key,event_type,thread_n,next_params):
    global data_lists
    global data_lista
    global data_listb
    global data_list0
    global data_list1
    
    headers = {"X-API-KEY": api_key}
    for m in range_run:
        
        wallet_address = df_opensea_totaladdress["token_owner_address"][m]
        nextpage = True
        count = 0
        
        if next_params:    
            next_param = next_params
        else:
            next_param = ""
        
        try:
            while nextpage:

                events_api = "https://api.opensea.io/api/v1/events?account_address="+ wallet_address + "&event_type=" + event_type + "&cursor=" + next_param
                response = requests.get(events_api, headers=headers)
                result_a_collection_events = json.loads(response.text)
                
                if "asset_events" in result_a_collection_events.keys() and result_a_collection_events["asset_events"]:
                
                    for i in range(len(result_a_collection_events["asset_events"])):
                    
                        if result_a_collection_events["asset_events"][i]["asset"]:
                            num_sales = result_a_collection_events["asset_events"][i]["asset"]["num_sales"]
                            token_id = result_a_collection_events["asset_events"][i]["asset"]["token_id"]
                            if result_a_collection_events["asset_events"][i]["asset"]["owner"]:
                                token_owner_address = result_a_collection_events["asset_events"][i]["asset"]["owner"]["address"]
                            else:
                                token_owner_address = result_a_collection_events["asset_events"][i]["asset"]["owner"]
                        else:
                            num_sales = ""
                            token_id = ""
                            token_owner_address = ""
                        
                        if result_a_collection_events["asset_events"][i]["asset_bundle"]:
                            asset_bundle = result_a_collection_events["asset_events"][i]["asset_bundle"]
                        else:
                            asset_bundle = ""
                        
                        event_timestamp = result_a_collection_events["asset_events"][i]["event_timestamp"]
                        event_type = result_a_collection_events["asset_events"][i]["event_type"]

                        listing_time = result_a_collection_events["asset_events"][i]["listing_time"]

                        if result_a_collection_events["asset_events"][i]["seller"]:
                            token_seller_address = result_a_collection_events["asset_events"][i]["seller"]["address"]
                        else:
                            token_seller_address = ""
                        
                        if result_a_collection_events["asset_events"][i]["total_price"]:
                            deal_price = int(result_a_collection_events["asset_events"][i]["total_price"])
                        else:
                            deal_price = ""
                                                
                        if result_a_collection_events["asset_events"][i]["payment_token"]:
                            payment_token_symbol = result_a_collection_events["asset_events"][i]["payment_token"]["symbol"]
                            payment_token_decimals = result_a_collection_events["asset_events"][i]["payment_token"]["decimals"]
                            payment_token_usdprice = np.float64(result_a_collection_events["asset_events"][i]["payment_token"]["usd_price"])
                        else:
                            payment_token_symbol = result_a_collection_events["asset_events"][i]["payment_token"]
                            payment_token_decimals = result_a_collection_events["asset_events"][i]["payment_token"]
                            payment_token_usdprice = result_a_collection_events["asset_events"][i]["payment_token"]
                        
                        quantity = result_a_collection_events["asset_events"][i]["quantity"]
                        starting_price = result_a_collection_events["asset_events"][i]["starting_price"]
                        ending_price = result_a_collection_events["asset_events"][i]["ending_price"]
                        approved_account = result_a_collection_events["asset_events"][i]["approved_account"]
                        auction_type = result_a_collection_events["asset_events"][i]["auction_type"]
                        bid_amount = result_a_collection_events["asset_events"][i]["bid_amount"]
                        
                        if result_a_collection_events["asset_events"][i]["transaction"]:    
                            transaction_hash = result_a_collection_events["asset_events"][i]["transaction"]["transaction_hash"]
                            block_hash = result_a_collection_events["asset_events"][i]["transaction"]["block_hash"]
                            block_number = result_a_collection_events["asset_events"][i]["transaction"]["block_number"]
                        else:
                            transaction_hash = ""
                            block_hash = ""
                            block_number = ""
                        
                        collection_slug = result_a_collection_events["asset_events"][i]["collection_slug"]
                        is_private = result_a_collection_events["asset_events"][i]["is_private"]
                        duration = result_a_collection_events["asset_events"][i]["duration"]
                        created_date = result_a_collection_events["asset_events"][i]["created_date"]
                        
                        contract_address = result_a_collection_events["asset_events"][i]["contract_address"]
                        wallet_address_input = df_opensea_totaladdress["token_owner_address"][m]
                        pagesnum = count
                        
                        data = [event_timestamp,event_type,token_id,num_sales,listing_time,token_owner_address,token_seller_address,deal_price, \
                                payment_token_symbol,payment_token_decimals,payment_token_usdprice,quantity,starting_price,ending_price,approved_account, \
                                asset_bundle,auction_type,bid_amount,transaction_hash,block_hash,block_number,is_private,duration,created_date,collection_slug,contract_address,wallet_address_input,pagesnum,"success",next_param]
                        
                        data_lis.append(data)
                        data_lists.append(data)
                        print("wallet: " + str(m) + " , pages: " + str(count) + ", "  + event_timestamp)
                        
                else:
                    
                    wallet_address_input = df_opensea_totaladdress["token_owner_address"][m]
                    pagesnum = count
                    #pages
                    data = ["","","","","","","","","","","","","","","",
                            "","","","","","","","","","","",wallet_address_input,pagesnum,"Fail-no asset_events",next_param]
                    data_lis.append(data)
                    data_lists.append(data)
                    
                    print(str(m) + " no asset_events!")
                    nextpage = False
                        
                if "next" in result_a_collection_events.keys():
                    if result_a_collection_events["next"]:
                        next_param = result_a_collection_events["next"]
                    else:
                        next_param = ""
                        nextpage = False
                else:
                    next_param = ""
                    nextpage = False
                count = count+1
                
        except:
            wallet_address_input = df_opensea_totaladdress["token_owner_address"][m]
            pagesnum = count
            data = ["","","","","","","","","","","","","","","",
                    "","","","","","","","","","","",wallet_address_input,pagesnum,"SOMTHING WRONG",next_param]
            
            data_lis.append(data)
            data_lists.append(data)
            
            if m==range_run[-1]+1:
                return "success"
            else:
                #記錄運行至檔案的哪一筆中斷與當前的cursor參數(next_param)
                rerun_range = range(m,range_run[-1]+1)
                if (thread_n % 2) == 0:
                    data_lista.append((rerun_range,next_param))
                else:
                    data_listb.append((rerun_range,next_param))
                return "fail"
        
        col = ["event_timestamp","event_type","token_id","num_sales","listing_time","token_owner_address","token_seller_address","deal_price", \
               "payment_token_symbol","payment_token_decimals","payment_token_usdprice","quantity","starting_price","ending_price","approved_account", \
                   "asset_bundle","auction_type","bid_amount","transaction_hash","block_hash","block_number","is_private","duration","created_date","collection_slug","contract_address","wallet_address_input","pages","msg","next_param"]
        #存檔，自己取名
        if (int(m)%50 == 0 and int(m)>0) or m == range_run[-1]:
            if (thread_n % 2) == 0:
                result_dfa = pd.DataFrame(data_lis, columns = col)
                result_dfa = result_dfa.reset_index(drop=True)
                result_dfa.to_excel("C:\\Users\\Fred\\Desktop + "\\coolcatsnft_0_" + str(m) + ".xlsx",encoding = "utf_8_sig")
            else:
                result_dfb = pd.DataFrame(data_lis, columns = col)
                result_dfb = result_dfb.reset_index(drop=True)
                result_dfb.to_excel("C:\\Users\\Fred\\Desktop + "\\coolcatsnft_1_" + str(m) + ".xlsx",encoding = "utf_8_sig")
        
    print("End   : " + str(datetime.datetime.now()))
    return "success"

#process_run的外層函數，當執行中斷時自動繼續往下執行
def controlfunc(process_run,range_run,df_opensea_totaladdress,data_lis,api_key,event_type,thread_n,next_params):
    global data_lista
    global data_listb
    global data_list0
    global data_list1
    
    s_f = process_run(range_run,df_opensea_totaladdress,data_lis,api_key,event_type,thread_n,next_params)
    
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
                    print("Rerun1 is preparing "+ str(count))
                    s_f = process_run(range1_rerun,df_opensea_totaladdress,data_lis,api_key,event_type,thread_n,data_lista[-1][1])
                    count = count+1
            else:
                if data_listb:
                    range2_rerun = data_listb[-1][0]
                    time.sleep(60)
                    print("Rerun2 is preparing "+ str(count))
                    s_f = process_run(range2_rerun,df_opensea_totaladdress,data_lis,api_key,event_type,thread_n,data_listb[-1][1])
                    count = count+1

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

if __name__  == '__main__':
    event_type = "successful"
    divide = 2
    range_s = 0
    range_e = 2
    api_key = ""
    api_key2 = ""
    
    data_lists = []
    data_lista = []
    data_listb = []
    range_collection = list(chunks(range(range_s, range_e), divide))
    thread = len(range_collection)

    start = str(datetime.datetime.now())
    for n in range(thread):
        globals()["datalist%s" %n] = []
        if (n % 2) == 0:
            globals()["add_thread%s" %n] = threading.Thread(target = controlfunc,args = (process_run,range_collection[n],df_opensea_totaladdress,globals()["datalist%s" %n],api_key,event_type,n,""))
            globals()["add_thread%s" %n].start()
        else:
            globals()["add_thread%s" %n] = threading.Thread(target = controlfunc,args = (process_run,range_collection[n],df_opensea_totaladdress,globals()["datalist%s" %n],api_key2,event_type,n,""))
            globals()["add_thread%s" %n].start()


    for nn in range(thread):
        globals()["add_thread%s" %nn].join()

print("Start :" + start)
print("End   : " + str(datetime.datetime.now()))


