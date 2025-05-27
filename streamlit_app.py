import os
import gdown
import pandas as pd
import streamlit as st
import pydeck as pdk

st.set_page_config(page_title="Olist Dashboard", layout="wide")

@st.cache_data(ttl=0)
def load_data():
    FILE_IDS = {
        "olist_category_name_translation.csv": "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
        "olist_sellers_dataset.csv":           "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        "olist_products_dataset.csv":          "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        "olist_orders_dataset.csv":            "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        "olist_order_reviews_dataset.csv":     "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        "olist_order_payments_dataset.csv":    "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        "olist_order_items_dataset.csv":       "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
        "olist_geolocation_dataset.csv":       "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        "olist_customers_dataset.csv":         "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
    }
    raw_dir = "data/raw"
    os.makedirs(raw_dir, exist_ok=True)

    for fname, fid in FILE_IDS.items():
        dest = os.path.join(raw_dir, fname)
        if not os.path.isfile(dest):
            gdown.download(f"https://drive.google.com/uc?export=download&id={fid}", dest, quiet=False)

    raw = {f: pd.read_csv(os.path.join(raw_dir, f)) for f in FILE_IDS}

    # Correct mappings
    cat_translate  = raw["olist_category_name_translation.csv"]
    seller_meta    = raw["olist_sellers_dataset.csv"]
    product_specs  = raw["olist_products_dataset.csv"]
    orders_meta    = raw["olist_orders_dataset.csv"]
    reviews_meta   = raw["olist_order_reviews_dataset.csv"]
    payments_meta  = raw["olist_order_payments_dataset.csv"]
    order_items    = raw["olist_order_items_dataset.csv"]
    geoloc_meta    = raw["olist_geolocation_dataset.csv"]
    customers_meta = raw["olist_customers_dataset.csv"]

    # 1) Build geo_summary
    geo_summary = (
        geoloc_meta
        .groupby("geolocation_zip_code_prefix")[["geolocation_lat","geolocation_lng"]]
        .mean()
        .reset_index()
    )

    # 2) Enrich customers
    customers = customers_meta.merge(
        geo_summary,
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left"
    )

    # 3) Start fact table
    df = order_items.copy()  # has order_item_id

    # 4) Merge order-level info
    df = df.merge(
        orders_meta[["order_id","customer_id","order_purchase_timestamp"]],
        on="order_id", how="left"
    )

    # 5) Merge customer metadata
    df = df.merge(customers, on="customer_id", how="left")

    # 6) Merge product specs
    df = df.merge(
        product_specs[["product_id","product_category_name"]],
        on="product_id", how="left"
    )

    # 7) Translate category
    df = df.merge(
        cat_translate, on="product_category_name", how="left"
    ).rename(columns={"product_category_name_english":"category_name"})

    # 8) Merge payments & reviews
    df = df.merge(
        payments_meta[["order_id","payment_type","payment_value"]],
        on="order_id", how="left"
    )
    df = df.merge(
        reviews_meta[["order_id","review_score"]],
        on="order_id", how="left"
    )

    # 9) Merge seller metadata (city/state)
    df = df.merge(seller_meta, on="seller_id", how="left")

    # 10) Type clean
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["payment_value"]            = pd.to_numeric(df["payment_value"], errors="coerce")
    df["geolocation_lat"]          = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"]          = pd.to_numeric(df["geolocation_lng"], errors="coerce")

    return df

df = load_data()

st.title("📊 Olist Customer Characterization Dashboard")

# KPIs
c1, c2, c3 = st.columns(3)
c1.metric("Total Orders",        f"{df['order_id'].nunique():,}")
c2.metric("Total Customers",     f"{df['customer_unique_id'].nunique():,}")
c3.metric("Total Revenue (BRL)", f"R$ {df['payment_value'].sum():,.2f}")

st.markdown("---")

# Top Products
tp = df["category_name"].value_counts().nlargest(10)
st.subheader("🏆 Top 10 Products")
st.bar_chart(tp)

# Top Buyers
tb = df.groupby("customer_unique_id")["payment_value"].sum().nlargest(10)
st.subheader("🛍️ Top 10 Buyers")
st.bar_chart(tb)

# Geomap
locs = df.groupby(["geolocation_lat","geolocation_lng"]).size().reset_index(name="Order Count")
st.subheader("📍 Purchase Locations")
st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    initial_view_state=pdk.ViewState(
        latitude=locs["geolocation_lat"].mean(),
        longitude=locs["geolocation_lng"].mean(),
        zoom=4
    ),
    layers=[pdk.Layer(
        "ScatterplotLayer",
        data=locs,
        get_position=["geolocation_lng","geolocation_lat"],
        get_radius="Order Count * 50",
        pickable=True
    )]
))

st.markdown("---")

# Payment Breakdown
st.subheader("💳 Payment Methods")
st.dataframe(df["payment_type"].value_counts().rename_axis("Method").reset_index(name="Count"))

st.markdown("---")

# Orders Over Time
df["month"] = df["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp()
st.subheader("📈 Orders Over Time")
st.line_chart(df.groupby("month").size().rename("Order Count"))
