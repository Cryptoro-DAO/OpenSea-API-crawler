# OpenSea API crawler

Source `Asset Events` data and engineer features to analyze NFT trading behavior aka FOMO

WIP

- Python script that retrieves assets events from OpenSea by `account_address`. The
extracted data can be saved locally or to AWS s3 via `boto3 / s3fs` package
- Procedure to load multiple Excel files into pandas DataFrame and save in feather format for fast subsequent loading
- Feature Engineering procedure
