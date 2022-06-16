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
