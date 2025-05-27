# streamlit_app.py

import os
import gdown
import pandas as pd
import streamlit as st
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
        # Customer metadata (99 441 × 5)
        "olist_category_name_translation.csv": "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        # Category translation (71 × 2)
        "olist_customers_dataset.csv":         "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
        "olist_geolocation_dataset.csv":       "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        "olist_order_items_dataset.csv":       "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        "olist_order_payments_dataset.csv":    "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        "olist_order_reviews_dataset.csv":     "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        "olist_orders_dataset.csv":            "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        "olist_products_dataset.csv":          "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
        "olist_sellers_dataset.csv":           "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
    }
    raw_dir = os.path.join("data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # 1) Download & verify
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

    # 2) Load each CSV
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

    # ── NEW: Print out each variable’s columns for sanity ────────────────────────
    st.write("### Verify variable mappings:")
    customers_meta = raw["olist_category_name_translation.csv"]
    cat_translate  = raw["olist_customers_dataset.csv"]
    seller_meta    = raw["olist_geolocation_dataset.csv"]
    orders_meta    = raw["olist_order_items_dataset.csv"]
    payments_meta  = raw["olist_order_payments_dataset.csv"]
    reviews_meta   = raw["olist_order_reviews_dataset.csv"]
    product_specs  = raw["olist_orders_dataset.csv"]
    order_products = raw["olist_products_dataset.csv"]
    sellers_df     = raw["olist_sellers_dataset.csv"]

    for var_name, df in [
        ("customers_meta", customers_meta),
        ("cat_translate",  cat_translate),
        ("seller_meta",    seller_meta),
        ("orders_meta",    orders_meta),
        ("payments_meta",  payments_meta),
        ("reviews_meta",   reviews_meta),
        ("product_specs",  product_specs),
        ("order_products", order_products),
        ("sellers_df",     sellers_df),
    ]:
        st.write(f"{var_name}: columns = {df.columns.tolist()} (shape={df.shape})")

    # 3) Enrich customers with geolocation
    geo_summary = (
        raw["olist_sellers_dataset.csv"]  # this should actually be order_reviews file
        .groupby("geolocation_zip_code_prefix")[["geolocation_lat","geolocation_lng"]]
        .mean()
        .reset_index()
    )
    st.write(f"geo_summary: columns={geo_summary.columns.tolist()}")

    # ... continue with merges & debug logs as before ...

    return pd.DataFrame()  # stub to keep app running

# ── Run & Display ──────────────────────────────────────────────────────────────
st.title("📊 Debugging Olist Data Pipeline")
_ = load_data()
