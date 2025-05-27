import os
import gdown

# ── Configuration ───────────────────────────────────────────────────────────────

# Map each CSV filename to its Google Drive file ID
FILE_IDS = {
    "olist_customers_dataset.csv":           "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
    "olist_geolocation_dataset.csv":         "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
    "olist_orders_dataset.csv":              "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
    "olist_order_items_dataset.csv":         "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
    "olist_order_payments_dataset.csv":      "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
    "olist_products_dataset.csv":            "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
    "olist_sellers_dataset.csv":             "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
    "olist_order_reviews_dataset.csv":       "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
    "olist_category_name_translation.csv":   "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
}

RAW_DIR = "data/raw"

# ── Download logic ──────────────────────────────────────────────────────────────

os.makedirs(RAW_DIR, exist_ok=True)

for fname, file_id in FILE_IDS.items():
    dest = os.path.join(RAW_DIR, fname)
    if not os.path.isfile(dest):
        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        print(f"Downloading {fname} from Drive into {dest}")
        gdown.download(url, dest, quiet=False)
    else:
        print(f"{fname} already exists — skipping download.")

# ── Now you can safely import pandas/streamlit and load your data ──────────────
import pandas as pd

customers = pd.read_csv(os.path.join(RAW_DIR, "olist_customers_dataset.csv"))
geoloc    = pd.read_csv(os.path.join(RAW_DIR, "olist_geolocation_dataset.csv"))
orders    = pd.read_csv(os.path.join(RAW_DIR, "olist_orders_dataset.csv"))
items     = pd.read_csv(os.path.join(RAW_DIR, "olist_order_items_dataset.csv"))
payments  = pd.read_csv(os.path.join(RAW_DIR, "olist_order_payments_dataset.csv"))
products  = pd.read_csv(os.path.join(RAW_DIR, "olist_products_dataset.csv"))
sellers   = pd.read_csv(os.path.join(RAW_DIR, "olist_sellers_dataset.csv"))
reviews   = pd.read_csv(os.path.join(RAW_DIR, "olist_order_reviews_dataset.csv"))
cats      = pd.read_csv(os.path.join(RAW_DIR, "olist_category_name_translation.csv"))

# …then continue with your data cleaning, merging, and dashboard code below.

