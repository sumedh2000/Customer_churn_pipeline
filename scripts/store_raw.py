import os
import glob
import shutil
from datetime import datetime

# Input directory (where Stage 2 saved files)
RAW_PATH = "raw_data/"

def partition_raw():
    files = glob.glob(os.path.join(RAW_PATH, "*.csv"))
    if not files:
        print("⚠ No CSV files found in raw_data/. Run stage 2 ingestion first.")
        return

    for f in files:
        fname = os.path.basename(f)

        # detect source
        if "github" in fname.lower():
            source = "github"
        elif "kaggle" in fname.lower():
            source = "kaggle"
        else:
            source = "other"

        # partitioned folder
        now = datetime.now()
        dest_dir = os.path.join(
            RAW_PATH, source,
            f"year={now.year}",
            f"month={now.strftime('%m')}",
            f"day={now.strftime('%d')}"
        )
        os.makedirs(dest_dir, exist_ok=True)

        dest_path = os.path.join(dest_dir, fname)

        # move file
        shutil.move(f, dest_path)
        print(f"[✔] Moved {fname} → {dest_path}")

if __name__ == "__main__":
    partition_raw()
