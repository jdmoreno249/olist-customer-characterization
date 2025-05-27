# streamlit_app.py

import os
import gdown
import pandas as pd
import streamlit as st

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Debug Olist Pipeline",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Data Download & Build with Debugging ────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=0)
def load_data():
    FILE_IDS = {
        # 1) Category translation (71×2)
        "olist_category_name_translation.csv": "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
        # 2) Sellers metadata (3095×4)
        "olist_sellers_dataset.csv":           "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        # 3) Product specs (32951×9)
        "olist_products_dataset.csv":          "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        # 4) Orders master (99441×8)
        "olist_orders_dataset.csv":            "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        # 5) Review scores (99224×7)
        "olist_order_reviews_dataset.csv":     "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        # 6) Payments (103886×5)
        "olist_order_payments_dataset.csv":    "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        # 7) Order‐items (112650×7)
        "olist_order_items_dataset.csv":       "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
        # 8) Geolocation lookup (1000163×5)
        "olist_geolocation_dataset.csv":       "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        # 9) Customer metadata (99441×5)
        "olist_customers_dataset.csv":         "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
    }
    raw_dir = os.path.join("data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # 1) Download & verify files
    for fname, fid in FILE_IDS.items():
        dest = os.path.join(raw_dir, fname)
        if not os.path.isfile(dest):
            st.write(f"Downloading {fname}...")
            gdown.download(
                f"https://drive.google.com/uc?export=download&id={fid}",
                dest,
                quiet=False
            )
        exists = os.path.isfile(dest)
        size = os.path.getsize(dest) if exists else 0
        st.write(f"{fname}: exists={exists}, size={size}")

    # 2) Load each CSV and print schema
    raw = {}
    for fname in FILE_IDS:
        path = os.path.join(raw_dir, fname)
        try:
            df = pd.read_csv(path)
            raw[fname] = df
            st.write(f"Loaded {fname}: shape={df.shape}, columns={df.columns.tolist()}")
        except Exception as e:
            st.write(f"ERROR loading {fname}: {e}")
            raw[fname] = pd.DataFrame()

    # ── Verify variable mappings ─────────────────────────────────────────────────
    st.write("### Verify variable mappings")
    cat_translate  = raw["olist_category_name_translation.csv"]
    seller_meta    = raw["olist_sellers_dataset.csv"]
    product_specs  = raw["olist_products_dataset.csv"]
    orders_meta    = raw["olist_orders_dataset.csv"]
    reviews_meta   = raw["olist_order_reviews_dataset.csv"]
    payments_meta  = raw["olist_order_payments_dataset.csv"]
    order_items    = raw["olist_order_items_dataset.csv"]
    geoloc_meta    = raw["olist_geolocation_dataset.csv"]
    customers_meta = raw["olist_customers_dataset.csv"]

    for var_name, df in [
        ("cat_translate",  cat_translate),
        ("seller_meta",    seller_meta),
        ("product_specs",  product_specs),
        ("orders_meta",    orders_meta),
        ("reviews_meta",   reviews_meta),
        ("payments_meta",  payments_meta),
        ("order_items",    order_items),
        ("geoloc_meta",    geoloc_meta),
        ("customers_meta", customers_meta),
    ]:
        st.write(f"{var_name}: shape={df.shape}, columns={df.columns.tolist()}")

    # Stop here for debugging
    return pd.DataFrame()

# ── Run Debug ─────────────────────────────────────────────────────────────────
st.title("📊 Debugging Olist Data Pipeline")
_ = load_data()
