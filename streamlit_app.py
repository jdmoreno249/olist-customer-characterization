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
@st.cache_data(show_spinner=False, ttl=0)  # disable cache persistence
def load_data():
    # Correct Drive IDs: ensure each CSV uses its own ID
    FILE_IDS = {
        "olist_category_name_translation.csv": "19YQGpVKifSM0qR04sCLUtiflz4RHX547",  # ~9 MB
        "olist_customers_dataset.csv":         "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",  # ~2.6 KB
        "olist_geolocation_dataset.csv":       "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        "olist_order_items_dataset.csv":       "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        "olist_order_payments_dataset.csv":    "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        "olist_order_reviews_dataset.csv":     "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        "olist_orders_dataset.csv":            "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        "olist_products_dataset.csv":          "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        "olist_sellers_dataset.csv":           "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
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
        size = os.path.getsize(dest) if exists else None
        st.write(f"File {fname} exists: {exists}, size: {size}")

    # 2) Load each CSV and print its schema
    raw = {}
    for fname in FILE_IDS:
        path = os.path.join(raw_dir, fname)
        try:
            df = pd.read_csv(path)
            raw[fname] = df
            st.write(f"Loaded {fname}: shape={df.shape}, columns={df.columns.tolist()}")
        except Exception as e:
            st.write(f"Error loading {fname}: {e}")
            raw[fname] = pd.DataFrame()

    # 3) Assign DataFrames
    customers_meta = raw["olist_category_name_translation.csv"]
    cat_translate  = raw["olist_customers_dataset.csv"]
    seller_meta    = raw["olist_geolocation_dataset.csv"]
    orders_meta    = raw["olist_order_items_dataset.csv"]
    payments_meta  = raw["olist_products_dataset.csv"]
    reviews_meta   = raw["olist_order_payments_dataset.csv"]
    geoloc_meta    = raw["olist_order_reviews_dataset.csv"]
    product_specs  = raw["olist_orders_dataset.csv"]
    sellers_df     = raw["olist_sellers_dataset.csv"]

    # 4) Enrich customers with geolocation
    geo_summary = (
        geoloc_meta
        .groupby("geolocation_zip_code_prefix")[["geolocation_lat","geolocation_lng"]]
        .mean()
        .reset_index()
    )
    st.write(f"geo_summary: shape={geo_summary.shape}, columns={geo_summary.columns.tolist()}")
    customers = customers_meta.merge(
        geo_summary,
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left"
    )
    st.write(f"After merging customers: shape={customers.shape}, columns={customers.columns.tolist()}")

    # 5) Build fact table starting from sellers_df
    df = sellers_df.copy()
    st.write(f"Start sellers_df: shape={df.shape}, columns={df.columns.tolist()}")

    # 6) Merge order-level info
    df = df.merge(
        orders_meta[["order_id","customer_id","order_purchase_timestamp"]],
        on="order_id",
        how="left"
    )
    st.write(f"After merging orders_meta: shape={df.shape}, columns={df.columns.tolist()}")

    # 7) Merge customers
    df = df.merge(customers, on="customer_id", how="left")
    st.write(f"After merging customers: shape={df.shape}, columns={df.columns.tolist()}")

    # 8) Merge product specs
    df = df.merge(
        product_specs[["product_id","product_category_name"]],
        on="product_id",
        how="left"
    )
    st.write(f"After merging product_specs: shape={df.shape}, columns={df.columns.tolist()}")

    # 9) Merge category translation
    df = df.merge(
        cat_translate[["product_category_name","product_category_name_english"]],
        on="product_category_name",
        how="left"
    ).rename(columns={"product_category_name_english":"category_name"})
    st.write(f"After merging cat_translate: shape={df.shape}, columns={df.columns.tolist()}")

    # 10) Merge payments & reviews
    df = df.merge(payments_meta[["order_id","payment_type","payment_value"]], on="order_id", how="left")
    st.write(f"After merging payments: shape={df.shape}, columns={df.columns.tolist()}")
    df = df.merge(reviews_meta[["order_id","review_score"]], on="order_id", how="left")
    st.write(f"After merging reviews: shape={df.shape}, columns={df.columns.tolist()}")

    # 11) Merge seller metadata
    df = df.merge(seller_meta, on="seller_id", how="left")
    st.write(f"After merging seller_meta: shape={df.shape}, columns={df.columns.tolist()}")

    # 12) Final cleaning
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["payment_value"]            = pd.to_numeric(df["payment_value"], errors="coerce")
    df["geolocation_lat"]          = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"]          = pd.to_numeric(df["geolocation_lng"], errors="coerce")
    st.write(f"Final df: shape={df.shape}, columns={df.columns.tolist()}")

    return df

# ── Run & Display ──────────────────────────────────────────────────────────────
st.title("📊 Debugging Olist Data Pipeline")
df = load_data()
st.dataframe(df.head(10))
