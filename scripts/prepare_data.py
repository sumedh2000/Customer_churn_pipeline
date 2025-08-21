import os, glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
import numpy as np

# ----------------------------
# CONFIG
# ----------------------------
RAW_PATH = "raw_data"
CLEAN_PATH = "data/clean"
EDA_PATH = "reports/eda"

os.makedirs(CLEAN_PATH, exist_ok=True)
os.makedirs(EDA_PATH, exist_ok=True)

# ----------------------------
# 1. Pick the latest ingested CSV
# ----------------------------
files = glob.glob(f"{RAW_PATH}/**/*.csv", recursive=True)
if not files:
    raise FileNotFoundError("❌ No raw CSVs found. Run ingestion + storage first.")

latest_file = max(files, key=os.path.getmtime)
print(f"📂 Using latest raw dataset: {latest_file}")

df = pd.read_csv(latest_file)

# --- After loading df ---


# Coerce numeric columns early (so they won't be treated as categorical)
for col in ["tenure", "MonthlyCharges", "TotalCharges", "SeniorCitizen"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Drop duplicates
df.drop_duplicates(inplace=True)

# Handle missing values
for col in df.select_dtypes(include=[np.number]).columns:
    df[col] = df[col].fillna(df[col].median())

for col in df.select_dtypes(include=["object"]).columns:
    df[col] = df[col].fillna(df[col].mode()[0])

# Map Churn to 0/1 and DO NOT one-hot it
if "Churn" in df.columns and df["Churn"].dtype == "object":
    df["Churn"] = df["Churn"].map({"Yes": 1, "No": 0}).astype(int)

# Define KNOWN categorical columns for Telco Churn
cat_cols = [
    "gender","Partner","Dependents","PhoneService","MultipleLines",
    "InternetService","OnlineSecurity","OnlineBackup","DeviceProtection",
    "TechSupport","StreamingTV","StreamingMovies","Contract",
    "PaperlessBilling","PaymentMethod"
]
# keep only those present
cat_cols = [c for c in cat_cols if c in df.columns]

# NEVER encode identifiers or the label
exclude = {"customerID","Churn"}
cat_cols = [c for c in cat_cols if c not in exclude]

# One-hot encode ONLY these known categoricals
df = pd.get_dummies(df, columns=cat_cols, drop_first=True)

# (Optional) scale numeric features except the label
from sklearn.preprocessing import StandardScaler
num_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c != "Churn"]
scaler = StandardScaler()
df[num_cols] = scaler.fit_transform(df[num_cols])

# ----------------------------
# 4. Scale numerical features
# ----------------------------
scaler = StandardScaler()
num_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
num_cols = [c for c in num_cols if c != "Churn"]  # skip target

df[num_cols] = scaler.fit_transform(df[num_cols])

# ----------------------------
# 5. Save cleaned dataset
# ----------------------------
clean_file = os.path.join(CLEAN_PATH, "cleaned_churn.csv")
df.to_csv(clean_file, index=False)
print(f"💾 Clean dataset saved → {clean_file}")

# ----------------------------
# 6. EDA plots
# ----------------------------
# Example numeric column to visualize
num_col = "MonthlyCharges" if "MonthlyCharges" in df.columns else num_cols[0]

plt.figure(figsize=(6,4))
sns.histplot(df[num_col], bins=30, kde=True)
plt.title(f"Histogram of {num_col}")
plt.savefig(os.path.join(EDA_PATH, f"{num_col}_hist.png"))
plt.close()

plt.figure(figsize=(6,4))
sns.boxplot(x=df[num_col])
plt.title(f"Boxplot of {num_col}")
plt.savefig(os.path.join(EDA_PATH, f"{num_col}_box.png"))
plt.close()

print(f"📊 EDA reports saved in {EDA_PATH}/")
