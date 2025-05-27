# streamlit_app.py

import os
import gdown
import sqlite3
import pandas as pd
import streamlit as st
import pydeck as pdk

# ── Page Configuration ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Olist Customer Characterization",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Data Fetching, Joining & Cleaning ──────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_data():
    # 1) Download raw CSVs from Google Drive if not already present
    FILE_IDS = {
        "olist_customers_dataset.csv":         "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
        "olist_geolocation_dataset.csv":       "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        "olist_orders_dataset.csv":            "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        "olist_order_items_dataset.csv":       "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        "olist_order_payments_dataset.csv":    "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        "olist_products_dataset.csv":          "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        "olist_sellers_dataset.csv":           "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
        "olist_order_reviews_dataset.csv":     "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        "olist_category_name_translation.csv": "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
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

    # 2) Load each CSV into a dict
    raw = {
        fname: pd.read_csv(os.path.join(raw_dir, fname))
        for fname in FILE_IDS
    }

    # 3) Rename according to actual content
    customers   = raw["olist_category_name_translation.csv"]
    cats        = raw["olist_customers_dataset.csv"]
    sellers     = raw["olist_geolocation_dataset.csv"]
    orders      = raw["olist_order_items_dataset.csv"]
    reviews     = raw["olist_order_payments_dataset.csv"]
    geoloc      = raw["olist_order_reviews_dataset.csv"]
    products    = raw["olist_orders_dataset.csv"]
    payments    = raw["olist_products_dataset.csv"]
    order_items = raw["olist_sellers_dataset.csv"]

    # 4) Push to SQLite in memory
    conn = sqlite3.connect(":memory:")
    for name, df in [
        ("customers",   customers),
        ("cats",        cats),
        ("sellers",     sellers),
        ("orders",      orders),
        ("reviews",     reviews),
        ("geoloc",      geoloc),
        ("products",    products),
        ("payments",    payments),
        ("order_items", order_items),
    ]:
        df.to_sql(name, conn, index=False, if_exists="replace")

    # 5) Build joined order_lines table via SQL
    order_lines_sql = """
    WITH customer_geo AS (
      SELECT
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        AVG(g.geolocation_lat) AS geolocation_lat,
        AVG(g.geolocation_lng) AS geolocation_lng
      FROM customers c
      JOIN geoloc g
        ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
      GROUP BY
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state
    )
    SELECT
      o.order_id,
      o.order_purchase_timestamp,
      cg.customer_id,
      cg.customer_unique_id,
      cg.customer_city,
      cg.customer_state,
      cg.geolocation_lat,
      cg.geolocation_lng,
      i.product_id,
      p.product_category_name      AS category_code,
      cat.product_category_name_english AS category_name,
      pay.payment_type,
      pay.payment_value,
      rev.review_score,
      s.seller_id,
      s.seller_zip_code_prefix,
      s.seller_city,
      s.seller_state
    FROM orders o
    LEFT JOIN customer_geo cg ON o.customer_id = cg.customer_id
    LEFT JOIN order_items i   ON o.order_id = i.order_id
    LEFT JOIN products p      ON i.product_id = p.product_id
    LEFT JOIN cats cat        ON p.product_category_name = cat.product_category_name
    LEFT JOIN payments pay    ON o.order_id = pay.order_id
    LEFT JOIN reviews rev     ON o.order_id = rev.order_id
    LEFT JOIN sellers s       ON i.seller_id = s.seller_id
    """
    df = pd.read_sql_query(order_lines_sql, conn, parse_dates=["order_purchase_timestamp"])

    # 6) Final cleaning
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
    df["geolocation_lat"] = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"] = pd.to_numeric(df["geolocation_lng"], errors="coerce")
    if "category_code" in df.columns:
        df = df.drop(columns=["category_code"])

    return df

df = load_data()


# ── Dashboard Sections ─────────────────────────────────────────────────────────

# KPIs
st.title("📊 Olist Customer Characterization Dashboard")
total_orders    = df["order_id"].nunique()
total_customers = df["customer_unique_id"].nunique()
total_revenue   = df["payment_value"].sum()

c1, c2, c3 = st.columns(3)
c1.metric("Total Orders",        f"{total_orders:,}")
c2.metric("Total Customers",     f"{total_customers:,}")
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
    df.groupby(["geolocation_lat", "geolocation_lng"])
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
    layers=[
        pdk.Layer(
            "ScatterplotLayer",
            data=locs,
            get_position=["geolocation_lng", "geolocation_lat"],
            get_radius="Order Count * 50",
            pickable=True
        )
    ]
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


