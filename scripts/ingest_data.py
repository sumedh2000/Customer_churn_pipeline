import os
import requests
import logging
import pandas as pd
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi

# ----------------
# CONFIG
# ----------------
logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
RAW_PATH = "./raw_data/"
os.makedirs(RAW_PATH, exist_ok=True)

# Log file inside raw_data
LOG_FILE = os.path.join(RAW_PATH, "churn_ingestion.log")

# API source 1 (IBM hosted CSV on GitHub)
API_SOURCE_1 = "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv"

# Kaggle dataset (Telecom Customer Churn)
KAGGLE_DATASET = "blastchar/telco-customer-churn"
KAGGLE_FILENAME = "WA_Fn-UseC_-Telco-Customer-Churn.csv"

# ----------------
# LOGGING
# ----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(message)s"
)

def ingest_git():
    try:
        response = requests.get(API_SOURCE_1, timeout=30)
        response.raise_for_status()
        filename = f"telecom_churn_github_{datetime.now().strftime('%Y%m%d')}.csv"
        filepath = os.path.join(RAW_PATH, filename)
        with open(filepath, "wb") as f:
            f.write(response.content)
        logging.info(f"SUCCESS - Ingested from GitHub → {filepath}")
        print(f"[✔] GitHub ingestion complete → {filename}")
        return filepath
    except Exception as e:
        logging.error(f"GitHub ingestion FAILED: {e}")
        print("[✘] GitHub ingestion failed:", str(e))
        return None

def ingest_kaggle():
    try:
        api = KaggleApi()
        api.authenticate()  # needs kaggle.json or env vars
        api.dataset_download_file(KAGGLE_DATASET, KAGGLE_FILENAME, path=RAW_PATH)
     
        filename = f"telecom_churn_kaggle_{datetime.now().strftime('%Y%m%d')}.csv"
        src_path = os.path.join(RAW_PATH, KAGGLE_FILENAME)
        dest_path = os.path.join(RAW_PATH, filename)
        os.rename(src_path, dest_path)
        logging.info(f"SUCCESS - Ingested from Kaggle → {dest_path}")
        print(f"[✔] Kaggle ingestion complete → {filename}")
        return dest_path
    except Exception as e:
        logging.error(f"Kaggle ingestion FAILED: {e}")
        print("[✘] Kaggle ingestion failed:", str(e))
        return None

if __name__ == "__main__":
    print("\nIngesting Telecom Churn data from 2 sources...\n")

    f1 = ingest_git()
    f2 = ingest_kaggle()

    # Quick verification
    if f1 and os.path.exists(f1):
        df1 = pd.read_csv(f1)
        print(" GitHub dataset shape:", df1.shape)
    if f2 and os.path.exists(f2):
        df2 = pd.read_csv(f2)
        print(" Kaggle dataset shape:", df2.shape)

    print(f"\n📂 All ingested files + logs saved in: {RAW_PATH}")
