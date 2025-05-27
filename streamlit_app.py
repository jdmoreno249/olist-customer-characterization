# streamlit_app.py

import os
import gdown
import pandas as pd
import streamlit as st
import pydeck as pdk

# ── Page Configuration ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Olist Customer Characterization",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Data Download & Build ──────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_data():
    # 1) Download raw CSVs if missing
    FILE_IDS = {
        "olist_category_name_translation.csv": "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
        "olist_customers_dataset.csv":         "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
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
    for fname, fid in FILE_IDS.items():
        dest = os.path.join(raw_dir, fname)
        if not os.path.isfile(dest):
            gdown.download(
                f"https://drive.google.com/uc?export=download&id={fid}",
                dest, quiet=True
            )

    # 2) Load all CSVs
    raw = {f: pd.read_csv(os.path.join(raw_dir, f)) for f in FILE_IDS}

    # 3) Map filenames → DataFrames by content
    customers_meta = raw["olist_category_name_translation.csv"]
    cat_translate  = raw["olist_customers_dataset.csv"]
    seller_meta    = raw["olist_geolocation_dataset.csv"]
    orders_meta    = raw["olist_order_items_dataset.csv"]
    payments_meta  = raw["olist_products_dataset.csv"]
    reviews_meta   = raw["olist_order_payments_dataset.csv"]
    geoloc_meta    = raw["olist_order_reviews_dataset.csv"]
    product_specs  = raw["olist_orders_dataset.csv"]
    sellers_df     = raw["olist_sellers_dataset.csv"]

    # 4) Enrich customers with average geolocation per zip
    geo_summary = (
        geoloc_meta
        .groupby("geolocation_zip_code_prefix")[["geolocation_lat","geolocation_lng"]]
        .mean()
        .reset_index()
    )
    customers = customers_meta.merge(
        geo_summary,
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left"
    )

    # ── Build fact table ──────────────────────────────────────────────────────────
    # a) Start from sellers_df so order_item_id exists
    df = sellers_df.copy()

    # b) Attach order-level data: customer_id & purchase timestamp
    df = df.merge(
        orders_meta[["order_id","customer_id","order_purchase_timestamp"]],
        on="order_id",
        how="left"
    )

    # c) Attach customer metadata + geolocation
    df = df.merge(customers, on="customer_id", how="left")

    # d) Attach product specs: category code
    df = df.merge(
        product_specs[["product_id","product_category_name"]],
        on="product_id",
        how="left"
    )

    # e) Translate category to English
    df = df.merge(
        cat_translate[["product_category_name","product_category_name_english"]],
        on="product_category_name",
        how="left"
    ).rename(columns={"product_category_name_english":"category_name"})

    # f) Attach payments and reviews
    df = df.merge(
        payments_meta[["order_id","payment_type","payment_value"]],
        on="order_id",
        how="left"
    )
    df = df.merge(
        reviews_meta[["order_id","review_score"]],
        on="order_id",
        how="left"
    )

    # g) Attach seller location metadata
    df = df.merge(seller_meta, on="seller_id", how="left")

    # 5) Final cleaning & type conversions
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["payment_value"]            = pd.to_numeric(df["payment_value"], errors="coerce")
    df["geolocation_lat"]          = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"]          = pd.to_numeric(df["geolocation_lng"], errors="coerce")

    return df

df = load_data()

# ── Dashboard ──────────────────────────────────────────────────────────────────
st.title("📊 Olist Customer Characterization Dashboard")

# KPIs
n_orders     = df["order_id"].nunique()
n_customers  = df["customer_unique_id"].nunique()
total_revenue= df["payment_value"].sum()

c1, c2, c3 = st.columns(3)
c1.metric("Total Orders",        f"{n_orders:,}")
c2.metric("Total Customers",     f"{n_customers:,}")
c3.metric("Total Revenue (BRL)", f"R$ {total_revenue:,.2f}")

st.markdown("---")

# Top 10 Products
st.subheader("🏆 Top 10 Products by Order Count")
top_products = (
    df["category_name"]
      .value_counts()
      .nlargest(10)
      .rename_axis("Product")
      .reset_index(name="Count")
)
st.bar_chart(top_products.set_index("Product")["Count"])

# Top 10 Buyers
st.subheader("🛍️ Top 10 Buyers by Lifetime Spend")
top_buyers = (
    df.groupby("customer_unique_id")["payment_value"]
      .sum()
      .nlargest(10)
      .reset_index(name="Total Spend")
)
st.bar_chart(top_buyers.set_index("customer_unique_id")["Total Spend"])

# Geospatial Map
st.subheader("📍 Purchase Locations")
locs = (
    df.groupby(["geolocation_lat","geolocation_lng"])
      .size()
      .reset_index(name="Order Count")
      .dropna()
)
deck = pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    initial_view_state=pdk.ViewState(
        latitude=locs["geolocation_lat"].mean(),
        longitude=locs["geolocation_lng"].mean(),
        zoom=4,
        pitch=0
    ),
    layers=[pdk.Layer(
        "ScatterplotLayer",
        data=locs,
        get_position=["geolocation_lng","geolocation_lat"],
        get_radius="Order Count * 50",
        pickable=True
    )]
)
st.pydeck_chart(deck)

# Payment Method Breakdown
st.subheader("💳 Payment Method Breakdown")
pm = df["payment_type"].value_counts().rename_axis("Method").reset_index(name="Count")
st.dataframe(pm)

st.markdown("---")

# Orders Over Time
st.subheader("📈 Orders Over Time")
df["month"] = df["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp()
orders_ts = df.groupby("month").size().rename("Order Count")
st.line_chart(orders_ts)
