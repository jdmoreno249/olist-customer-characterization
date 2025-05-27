# streamlit_app.py

import os
import gdown
import pandas as pd
import streamlit as st   # must import before using @st.cache_data
import pydeck as pdk

# ── Page Configuration ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Debug Olist Pipeline",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Data Download & Build with Debugging ────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=0)
def load_data():
    FILE_IDS = {
        # 1) Category translation (product → English)
        "olist_category_name_translation.csv": "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
        # 2) Sellers (order_item_id + seller info)
        "olist_sellers_dataset.csv":           "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        # 3) Product specs (product_id → category_code + dims)
        "olist_products_dataset.csv":          "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        # 4) Orders master (order-level timestamps + customer_id)
        "olist_orders_dataset.csv":            "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        # 5) Review scores
        "olist_order_reviews_dataset.csv":     "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        # 6) Payments
        "olist_order_payments_dataset.csv":    "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        # 7) Order-items details
        "olist_order_items_dataset.csv":       "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
        # 8) Geolocation lookup
        "olist_geolocation_dataset.csv":       "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        # 9) Customer metadata
        "olist_customers_dataset.csv":         "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
    }
    raw_dir = os.path.join("data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Download & log
    for fname, fid in FILE_IDS.items():
        dest = os.path.join(raw_dir, fname)
        if not os.path.isfile(dest):
            st.write(f"Downloading {fname}...")
            gdown.download(
                f"https://drive.google.com/uc?export=download&id={fid}",
                dest, quiet=False
            )
        exists = os.path.isfile(dest)
        size = os.path.getsize(dest) if exists else 0
        st.write(f"{fname}: exists={exists}, size={size}")

    # Load & log schemas
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

    # Verify variable mappings
    st.write("### Verify variable mappings")
    mapping = [
        ("cat_translate",  "olist_category_name_translation.csv"),
        ("sellers_df",     "olist_sellers_dataset.csv"),
        ("product_specs",  "olist_products_dataset.csv"),
        ("orders_meta",    "olist_orders_dataset.csv"),
        ("reviews_meta",   "olist_order_reviews_dataset.csv"),
        ("payments_meta",  "olist_order_payments_dataset.csv"),
        ("order_items",    "olist_order_items_dataset.csv"),
        ("geoloc_meta",    "olist_geolocation_dataset.csv"),
        ("customers_meta", "olist_customers_dataset.csv"),
    ]
    for var_name, fname in mapping:
        df = raw[fname]
        st.write(f"{var_name}: shape={df.shape}, columns={df.columns.tolist()}")

    return raw  # stop here so we can inspect

# ── Run Debug ───────────────────────────────────────────────────────────────────
st.title("📊 Debugging Olist Data Pipeline")
_ = load_data()
