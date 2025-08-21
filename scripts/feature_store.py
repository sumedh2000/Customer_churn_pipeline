# scripts/feature_store.py
import os
import json
import argparse
import sqlite3
from datetime import datetime
import pandas as pd

DB_PATH = "data/customer_churn.db"
TABLE = "customers_transformed"
CATALOG_JSON = "feature_store/metadata.json"
CATALOG_EXPORT = "reports/feature_catalog.csv"
OUT_DIR = "data/feature_sets"

os.makedirs("reports", exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

def _load_catalog():
    if not os.path.exists(CATALOG_JSON):
        raise FileNotFoundError(f"Missing {CATALOG_JSON}. Create metadata.json first.")
    with open(CATALOG_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def _connect():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Missing DB at {DB_PATH}. Run Stage 6 first.")
    return sqlite3.connect(DB_PATH)

def _table_columns(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return cols

def cmd_register(args):
    """Validate metadata against DB and export human-readable catalog CSV."""
    catalog = _load_catalog()
    conn = _connect()
    try:
        cols = _table_columns(conn, TABLE)
        rows = []
        for f in catalog["features"]:
            name = f["name"]
            present = "yes" if name in cols else "no"
            rows.append({
                "feature": name,
                "dtype": f.get("dtype", ""),
                "entity": f.get("entity", ""),
                "version": f.get("version", ""),
                "source_table": f.get("source_table", ""),
                "in_db": present,
                "description": f.get("description", "")
            })
        df = pd.DataFrame(rows)
        df.to_csv(CATALOG_EXPORT, index=False)
        print(f"✅ Catalog exported → {CATALOG_EXPORT}")
        print(df)
    finally:
        conn.close()

def cmd_list(args):
    """List available features (metadata + DB presence)."""
    catalog = _load_catalog()
    conn = _connect()
    try:
        cols = set(_table_columns(conn, TABLE))
        for f in catalog["features"]:
            flag = "✓" if f["name"] in cols else "✗"
            print(f"{flag} {f['name']} (v{f.get('version','')}) — {f.get('description','')}")
    finally:
        conn.close()

def _validate_features(requested, available_cols):
    missing = [f for f in requested if f not in available_cols]
    if missing:
        raise ValueError(f"Requested features not in table '{TABLE}': {missing}")

def cmd_training(args):
    """Build offline training set CSV for the requested features (+ label if included)."""
    feats = args.features
    if not feats:
        raise ValueError("Provide at least one feature via --features")

    entity_id = "customerID"
    conn = _connect()
    try:
        cols = _table_columns(conn, TABLE)
        # ensure entity id always included
        requested = [entity_id] + feats
        _validate_features(requested, cols)

        df = pd.read_sql(f"SELECT {', '.join(requested)} FROM {TABLE}", conn)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = os.path.join(OUT_DIR, f"training_{ts}.csv")
        df.to_csv(out, index=False)
        print(f"✅ Training set created → {out}")
        print(df.head())
    finally:
        conn.close()

def cmd_online(args):
    """Fetch online features for given customer IDs."""
    feats = args.features
    ids = args.ids
    if not feats:
        raise ValueError("Provide features via --features")
    if not ids:
        raise ValueError("Provide one or more customer IDs via --ids")

    entity_id = "customerID"
    conn = _connect()
    try:
        cols = _table_columns(conn, TABLE)
        requested = [entity_id] + feats
        _validate_features(requested, cols)

        # parameterized query
        placeholders = ", ".join(["?"] * len(ids))
        sql = f"SELECT {', '.join(requested)} FROM {TABLE} WHERE {entity_id} IN ({placeholders})"
        df = pd.read_sql(sql, conn, params=ids)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = os.path.join(OUT_DIR, f"online_{ts}.csv")
        df.to_csv(out, index=False)
        print(f"✅ Online features saved → {out}")
        print(df)
    finally:
        conn.close()

def cmd_sample_ids(args):
    """Show sample customer IDs from the table to help test 'online'."""
    n = args.limit
    entity_id = "customerID"
    conn = _connect()
    try:
        df = pd.read_sql(f"SELECT {entity_id} FROM {TABLE} LIMIT {n};", conn)
        print(df[entity_id].tolist())
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Simple Feature Store CLI")
    sub = parser.add_subparsers(required=True)

    p0 = sub.add_parser("register", help="Validate metadata and export catalog CSV")
    p0.set_defaults(func=cmd_register)

    p1 = sub.add_parser("list", help="List features from metadata and DB availability")
    p1.set_defaults(func=cmd_list)

    p2 = sub.add_parser("training", help="Build offline training set CSV")
    p2.add_argument("--features", nargs="+", required=True, help="Feature names to include")
    p2.set_defaults(func=cmd_training)

    p3 = sub.add_parser("online", help="Fetch online features for customer IDs")
    p3.add_argument("--features", nargs="+", required=True, help="Feature names to include")
    p3.add_argument("--ids", nargs="+", required=True, help="Customer IDs")
    p3.set_defaults(func=cmd_online)

    p4 = sub.add_parser("sample_ids", help="Show some example customer IDs")
    p4.add_argument("--limit", type=int, default=5)
    p4.set_defaults(func=cmd_sample_ids)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
