# ── Data Download & Build with Debugging ────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=0)
def load_data():
    FILE_IDS = {
        # 1) Category translation
        "olist_category_name_translation.csv": "1wTlgBc515BR2DR5Wgff0Cd-XFuHwb80V",
        # 2) Sellers (order_items + seller info)
        "olist_sellers_dataset.csv":           "1s_L2-JC6MobsEmKNBQ41ezbCe6V3YrY4",
        # 3) Products (product specs)
        "olist_products_dataset.csv":          "1Ux4yYn90rHv1gZBk-L2CgdZcNtEiabzD",
        # 4) Orders (order_items master)
        "olist_orders_dataset.csv":            "1MqAAQcsyPV204GdnLHofHn1U8lJ4TYG4",
        # 5) Order reviews (review scores)
        "olist_order_reviews_dataset.csv":     "1koSHpwLEkbZ3Q4M5qxdn8vDBOqWxpefn",
        # 6) Order payments (payment info)
        "olist_order_payments_dataset.csv":    "1HHia6OiZA084ejjLIFm4qqyC_6df1FHh",
        # 7) Order‐items (timestamps & customer_id)
        "olist_order_items_dataset.csv":       "1PYUU0pdkAE7xm1nXFFkIDHiR0-_VPuCt",
        # 8) Geolocation (customer zip → lat/lng)
        "olist_geolocation_dataset.csv":       "1GyDACu8Jt2DFA6ldl1BshL9_qpvSJsYb",
        # 9) Customers (customer metadata)
        "olist_customers_dataset.csv":         "19YQGpVKifSM0qR04sCLUtiflz4RHX547",
    }
    raw_dir = os.path.join("data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Download & verify each file
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
    st.write("### Verify variable mappings:")
    for var_name, fname in [
        ("cat_translate",  "olist_category_name_translation.csv"),
        ("sellers_df",     "olist_sellers_dataset.csv"),
        ("product_specs",  "olist_products_dataset.csv"),
        ("orders_spec",    "olist_orders_dataset.csv"),
        ("reviews_meta",   "olist_order_reviews_dataset.csv"),
        ("payments_meta",  "olist_order_payments_dataset.csv"),
        ("orders_meta",    "olist_order_items_dataset.csv"),
        ("geoloc_meta",    "olist_geolocation_dataset.csv"),
        ("customers_meta", "olist_customers_dataset.csv"),
    ]:
        df = raw[fname]
        st.write(f"{var_name}: shape={df.shape}, columns={df.columns.tolist()}")

    return raw  # stub, stop here so we can verify
