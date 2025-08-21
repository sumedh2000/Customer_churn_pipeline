import os
import glob
import pandas as pd
import logging

# ----------------
# CONFIG
# ----------------
RAW_PATH = "raw_data/"
REPORT_PATH = "reports/"
LOG_PATH = "logs/"

os.makedirs(REPORT_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "validation.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------
# VALIDATION
# ----------------
def validate_csv(file_path):
    issues = []
    try:
        df = pd.read_csv(file_path)

        # Required columns
        if "customerID" not in df.columns:
            issues.append("Missing column: customerID")
        if "Churn" not in df.columns:
            issues.append("Missing column: Churn")

        # Missing values
        nulls = df.isnull().sum().sum()
        if nulls > 0:
            issues.append(f"Contains {nulls} missing values")

        # Duplicates
        dups = df.duplicated().sum()
        if dups > 0:
            issues.append(f"Contains {dups} duplicate rows")

        # Data types (example check)
        if "tenure" in df.columns and not pd.api.types.is_integer_dtype(df["tenure"]):
            issues.append("Column tenure should be integer")

    except Exception as e:
        issues.append(f"Error reading file: {e}")

    return issues

# ----------------
# MAIN
# ----------------
def main():
    pattern = os.path.join(RAW_PATH, "**/*.csv")
    files = glob.glob(pattern, recursive=True)

    if not files:
        raise FileNotFoundError("No CSV files found under raw_data/. Run ingestion first.")

    report = []
    for f in files:
        issues = validate_csv(f)
        if issues:
            for issue in issues:
                logging.warning(f"{os.path.basename(f)} | {issue}")
                report.append([os.path.basename(f), "FAILED", issue])
        else:
            logging.info(f"{os.path.basename(f)} | Validation Passed")
            report.append([os.path.basename(f), "PASSED", "No issues found"])

    # Save report
    df_report = pd.DataFrame(report, columns=["SourceFile", "Status", "Issue"])
    df_report.to_csv(os.path.join(REPORT_PATH, "data_quality_report.csv"), index=False)

    print("✅ Validation complete → reports/data_quality_report.csv")

if __name__ == "__main__":
    main()
