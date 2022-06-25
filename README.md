# OpenSea API crawler

Source `Asset Events` and engineer features to analyze NFT trading behavior,
e.g. FOMO.
See also the companion project [OpenSea_EDA](https://github.com/Cryptoro-DAO/OpenSea_EDA).

WIP

- data_crawler module, a python script that retrieves assets events from
OpenSea by `account_address`. The extracted data can be saved locally or to AWS
s3 via `boto3 / s3fs` package
- Procedure to load multiple Excel files into pandas DataFrame and save in
feather format for fast subsequent loading
- Procedure to engineering feature for building prediction and classification
models

# Wallet Features

attribute|type|description 
--|--|--
age|int|length of transaction history;<br/>also available at the NFT collection level
mean_duration|int|average difference in seconds between the time a token is listed and the time the sales is completed;<br/>also available at the NFT collection level
num_nft|int|number of NFT having owned since the beginning of the available wallet history
num_collection|int|number of collections having owned since the beginning of available wallet history
num_nft_onhand|int|the number of NFT currently on-hand;<br/>also available at the NFT collection level
num_collect_onhand|int|the number of collections on-hand. The number of NFT on-hand the collection must be greater than 0.
mean_duration_held|timedelta|average time NFT held before selling; the average time between buy and sell
endurance_rank|float|the percentage rank of the average hold period at the wallet level measured using the entire population included in this analysis
median_buy_usd</br>median_sell_usd<br/> total_buy_usd</br>total_sell_usd|float|median and total USD for buying or selling NFT
buy_xact<br/>sell_xact<br/>|int|the number of _successful_ buy or sell transaction
quantity_buy<br/>quantity_sell|int|the quantity of NFT bought or sold
profit_usd|float| profit or lost to date in USD; <br/>Profit or lost is defined as the difference in purchasing an NFT preceding the selling of the same token by the same wallet. Proceeds from repeated purchasing and selling of the same token are added together
cost_usd|float|cost basis in USD of the price paid for the NFT sold
win<br/>lose<br/>draw|float|counts to date of the NFT that are either sold at higher price than purchased, below price, or same
win_ratio<br/>lose_ratio|float|win-lose ratios to date
profit_ratio|float|profit_usd / cost_usd