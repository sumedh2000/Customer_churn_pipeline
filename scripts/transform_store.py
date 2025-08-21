# scripts/transform_store.py
import os
import sys
import glob
import logging
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np

# ----------------------------
# CONFIG & FOLDERS
# ----------------------------
CLEAN_INPUT_DIR = "data/clean"                 # from Stage 5
TRANS_OUT_DIR   = "data/transformed"           # transformed CSV output
DB_PATH         = "data/customer_churn.db"     # SQLite DB path
REPORTS_DIR     = "reports"
LOGS_DIR        = "logs"

# Keep a safe margin under SQLite's 999-parameter limit
MAX_SQLITE_COLS = 950

os.makedirs(TRANS_OUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOGS_DIR, "transform_store.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# UTILS
# ----------------------------
def _latest_clean_csv():
    candidates = glob.glob(os.path.join(CLEAN_INPUT_DIR, "*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No cleaned CSVs found under {CLEAN_INPUT_DIR}/. "
            "Run Stage 5 (prepare_data) first."
        )
    return max(candidates, key=os.path.getmtime)

def _to_numeric_safe(series):
    """Coerce to numeric, set invalid to NaN."""
    return pd.to_numeric(series, errors="coerce")

def _safe_div(a, b):
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(b == 0, np.nan, a / b)
    return out

def _infer_sql_type(dtype):
    """Map pandas dtype to SQLite column type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    if pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"
    return "TEXT"

# ----------------------------
# FEATURE ENGINEERING
# ----------------------------
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure ID & label exist in a sane format
    if "customerID" not in df.columns:
        raise ValueError("Expected 'customerID' column in cleaned data.")
    df["customerID"] = df["customerID"].astype(str)

    # Normalize Churn to 0/1 if needed
    if "Churn" in df.columns:
        if df["Churn"].dtype == object:
            df["Churn"] = df["Churn"].map({"Yes": 1, "No": 0})
        df["Churn"] = df["Churn"].fillna(0).astype(int)

    # Coerce common numeric columns if present
    for col in ["MonthlyCharges", "TotalCharges", "tenure"]:
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])

    # TotalSpend: prefer provided TotalCharges, else MonthlyCharges * tenure
    if "TotalCharges" in df.columns and df["TotalCharges"].notna().any():
        df["TotalSpend"] = df["TotalCharges"]
    elif all(c in df.columns for c in ["MonthlyCharges", "tenure"]):
        df["TotalSpend"] = (df["MonthlyCharges"] * df["tenure"]).astype(float)
    else:
        df["TotalSpend"] = np.nan

    # AvgMonthlySpend = TotalSpend / tenure
    if "tenure" in df.columns:
        df["AvgMonthlySpend"] = _safe_div(df["TotalSpend"], df["tenure"])
    else:
        df["AvgMonthlySpend"] = np.nan

    # TenureGroup buckets
    if "tenure" in df.columns:
        bins = [-np.inf, 6, 12, 24, 48, np.inf]
        labels = ["0-6", "7-12", "13-24", "25-48", "48+"]
        df["TenureGroup"] = pd.cut(df["tenure"], bins=bins, labels=labels)
    else:
        df["TenureGroup"] = "Unknown"

    # NumAddons: count *_Yes flags (typical after one-hot)
    yes_cols = [c for c in df.columns if c.endswith("_Yes")]
    if yes_cols:
        df["NumAddons"] = df[yes_cols].sum(axis=1).astype(float)
    else:
        df["NumAddons"] = 0.0

    # Fill any residual inf values
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Final light imputation for engineered cols
    for col in ["TotalSpend", "AvgMonthlySpend", "NumAddons"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Keep a stable column order: ID, label, engineered, then the rest
    front = [c for c in ["customerID", "Churn", "TotalSpend", "AvgMonthlySpend", "TenureGroup", "NumAddons"] if c in df.columns]
    others = [c for c in df.columns if c not in front]
    df = df[front + others]

    return df

# ----------------------------
# DB WRITE + SCHEMA/QUERIES
# ----------------------------
def write_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "customers_transformed"):
    conn = sqlite3.connect(db_path)
    try:
        # Write table
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Add basic indexes (customerID, label if present)
        cur = conn.cursor()
        if "customerID" in df.columns:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_customerID ON {table_name}(customerID);")
        if "Churn" in df.columns:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_churn ON {table_name}(Churn);")
        conn.commit()
    finally:
        conn.close()

def write_schema_file(df: pd.DataFrame, out_path: str, table_name: str = "customers_transformed"):
    lines = [f"DROP TABLE IF EXISTS {table_name};", f"CREATE TABLE {table_name} ("]
    cols_sql = []
    for col in df.columns:
        sql_type = _infer_sql_type(df[col].dtype)
        cols_sql.append(f"  {col} {sql_type}")
    lines.append(",\n".join(cols_sql))
    lines.append(");")
    schema_sql = "\n".join(lines)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(schema_sql)

def write_sample_queries(out_path: str, table_name: str = "customers_transformed"):
    sample = f"""-- Sample analytics queries
-- 1) Churn rate
SELECT ROUND(100.0 * AVG(CAST(Churn AS REAL)), 2) AS churn_rate_pct
FROM {table_name};

-- 2) Average spend by tenure group
SELECT TenureGroup, ROUND(AVG(AvgMonthlySpend), 2) AS avg_monthly_spend
FROM {table_name}
GROUP BY TenureGroup
ORDER BY 
  CASE TenureGroup 
    WHEN '0-6' THEN 1
    WHEN '7-12' THEN 2
    WHEN '13-24' THEN 3
    WHEN '25-48' THEN 4
    WHEN '48+' THEN 5
    ELSE 6
  END;

-- 3) Top 10 customers by TotalSpend
SELECT customerID, TotalSpend
FROM {table_name}
ORDER BY TotalSpend DESC
LIMIT 10;
"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(sample)

def write_summary(out_path: str, src_file: str, rows: int, cols: int, features_added: list):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt = f"""Transformation Summary
Timestamp        : {ts}
Source           : {src_file}
Rows x Cols      : {rows} x {cols}
Engineered feats : {", ".join(features_added)}

Notes:
- Churn normalized to 0/1 if needed.
- TotalSpend uses TotalCharges when available; else MonthlyCharges * tenure.
- AvgMonthlySpend = TotalSpend / tenure (0 handled as NaN then filled 0).
- TenureGroup buckets: 0-6, 7-12, 13-24, 25-48, 48+
- NumAddons counts *_Yes one-hot columns if present.
"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(txt)

# ----------------------------
# MAIN
# ----------------------------
def main():
    try:
        src = _latest_clean_csv()
        print(f"Using cleaned input: {src}")
        logging.info(f"Stage6 start | source={src}")

        df = pd.read_csv(src)
        before_shape = df.shape

        df_tr = engineer_features(df)

        # --- Column cap to satisfy SQLite limits ---
        base_keep = [
            "customerID", "Churn",
            "TotalSpend", "AvgMonthlySpend", "TenureGroup", "NumAddons",
            "tenure", "MonthlyCharges", "TotalCharges"
        ]
        base_keep = [c for c in base_keep if c in df_tr.columns]

        numeric_others = [c for c in df_tr.select_dtypes(include=[np.number]).columns
                          if c not in base_keep]
        other_others = [c for c in df_tr.columns
                        if c not in base_keep and c not in numeric_others]

        keep_cols = base_keep[:]
        for pool in (numeric_others, other_others):
            for c in pool:
                if len(keep_cols) >= MAX_SQLITE_COLS:
                    break
                keep_cols.append(c)
            if len(keep_cols) >= MAX_SQLITE_COLS:
                break

        if len(df_tr.columns) > len(keep_cols):
            dropped = [c for c in df_tr.columns if c not in keep_cols]
            print(f"⚠ Reducing columns: keeping {len(keep_cols)}, dropping {len(dropped)} (SQLite limit).")
            logging.warning(f"Column cap applied | kept={len(keep_cols)} dropped={len(dropped)}")
            df_tr = df_tr[keep_cols]

        after_shape = df_tr.shape
        print("Final transformed shape:", after_shape)

        # Persist transformed CSV
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_csv = os.path.join(TRANS_OUT_DIR, f"transformed_customers_{ts}.csv")
        df_tr.to_csv(out_csv, index=False)

        # Write to SQLite
        write_sqlite(df_tr, DB_PATH, table_name="customers_transformed")

        # Emit schema and queries
        schema_path = os.path.join(REPORTS_DIR, "schema_sqlite.sql")
        write_schema_file(df_tr, schema_path, table_name="customers_transformed")

        queries_path = os.path.join(REPORTS_DIR, "sample_queries.sql")
        write_sample_queries(queries_path, table_name="customers_transformed")

        # Summary
        summary_path = os.path.join(REPORTS_DIR, "transformation_summary.txt")
        engineered = [c for c in ["TotalSpend", "AvgMonthlySpend", "TenureGroup", "NumAddons"] if c in df_tr.columns]
        write_summary(summary_path, os.path.basename(src), after_shape[0], after_shape[1], engineered)

        logging.info(f"Stage6 complete | out_csv={out_csv} | db={DB_PATH} | rows={after_shape[0]} cols={after_shape[1]}")
        print("✅ Stage 6 complete")
        print("  • CSV  →", out_csv)
        print("  • DB   →", DB_PATH, "(table: customers_transformed)")
        print("  • SQL  →", schema_path, "and", queries_path)
        print("  • Note →", summary_path)

    except Exception as e:
        logging.exception(f"Stage6 failed: {e}")
        print("❌ Stage 6 failed:", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
