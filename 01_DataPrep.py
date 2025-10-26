import zipfile
import os
import pandas as pd
from collections import Counter

zip_path = os.path.expanduser("/Users/gizemmervedemir/Downloads/archive.zip")

with zipfile.ZipFile(zip_path, "r") as z:
    print(" Files in ZIP:")
    for file in z.namelist():
        if file.endswith(".csv"):
            print(" -", file)

with zipfile.ZipFile(zip_path, "r") as z:
    for file in z.namelist():
        if file.endswith(".csv"):
            with z.open(file) as f:
                df = pd.read_csv(f, nrows=5)
                print(f"\n {file}")
                print(df.head())
                print("Columns:", df.columns.tolist())


ZIP_PATH = os.path.expanduser("/Users/gizemmervedemir/Downloads/archive.zip")  # path to your ZIP
OUTPUT_CSV = os.path.expanduser("/Users/gizemmervedemir/ACM476/data_ready_for_cs.csv")

REQUIRED = {
    "olist_orders_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_customers_dataset.csv",
}
OPTIONAL = {
    "olist_products_dataset.csv",
    "product_category_name_translation.csv",
}

#	kwargs → short for “keyword arguments.”
#	The ** operator means: “take all keyword-style arguments and pack them into a dictionary.”

#You use **kwargs when:
	# You don’t know how many keyword arguments will be passed in.
	# You want your function to be flexible and accept extra optional parameters.

def read_csv_from_zip(zf, name, **kwargs):
    with zf.open(name) as f:
        return pd.read_csv(f, **kwargs)

def mode_safe(series):
    if series.isna().all() or len(series) == 0:
        return pd.NA
    counts = Counter(series.dropna())
    return counts.most_common(1)[0][0] if counts else pd.NA

with zipfile.ZipFile(ZIP_PATH, "r") as z:

    names = set(z.namelist())
    missing = REQUIRED - names
    if missing:
        raise FileNotFoundError(f"Missing files in ZIP: {missing}")

    orders   = read_csv_from_zip(z, "olist_orders_dataset.csv")
    reviews  = read_csv_from_zip(z, "olist_order_reviews_dataset.csv")
    items    = read_csv_from_zip(z, "olist_order_items_dataset.csv")
    pays     = read_csv_from_zip(z, "olist_order_payments_dataset.csv")
    custs    = read_csv_from_zip(z, "olist_customers_dataset.csv")

    products = read_csv_from_zip(z, "olist_products_dataset.csv") if "olist_products_dataset.csv" in names else None
    trans = read_csv_from_zip(z, "product_category_name_translation.csv") if "product_category_name_translation.csv" in names else None

# --- Date parsing ---
date_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for c in date_cols:
    if c in orders.columns:
        orders[c] = pd.to_datetime(orders[c], errors="coerce")

# --- Reviews cleanup ---
if "review_answer_timestamp" in reviews.columns:
    reviews["review_answer_timestamp"] = pd.to_datetime(reviews["review_answer_timestamp"], errors="coerce")
reviews = reviews.sort_values("review_answer_timestamp").drop_duplicates("order_id", keep="last")[["order_id", "review_score"]]

# --- Items aggregate ---
items_agg = (
    items.groupby("order_id")
    .agg(
        item_lines=("order_item_id", "count"),
        total_price=("price", "sum"),
        total_freight=("freight_value", "sum"),
        distinct_products=("product_id", "nunique"),
        distinct_sellers=("seller_id", "nunique"),
    )
    .reset_index()
)

# --- Optional: product categories ---
if products is not None:
    products = products[["product_id", "product_category_name"]]
    if trans is not None and {"product_category_name", "product_category_name_english"} <= set(trans.columns):
        products = products.merge(
            trans[["product_category_name", "product_category_name_english"]],
            on="product_category_name",
            how="left",
        )
    items_for_cat = items[["order_id", "product_id"]].merge(products, on="product_id", how="left")
    cat_col = "product_category_name_english" if "product_category_name_english" in items_for_cat.columns else "product_category_name"
    cat_per_order = (
        items_for_cat.groupby("order_id")[cat_col]
        .agg(mode_safe)
        .rename("dominant_category")
        .reset_index()
    )
else:
    cat_per_order = None

# --- Payments aggregate ---
pays_agg = (
    pays.groupby("order_id")
    .agg(
        total_payment=("payment_value", "sum"),
        primary_payment_type=("payment_type", mode_safe),
        payment_types_count=("payment_type", "nunique"),
    )
    .reset_index()
)

# --- Base (orders) ---
base = orders.copy()
base["delivery_delay_days"] = (
    (base["order_delivered_customer_date"] - base["order_estimated_delivery_date"]).dt.days
)
base["order_purchase_date"] = base["order_purchase_timestamp"].dt.date
base["order_purchase_hour"] = base["order_purchase_timestamp"].dt.hour
base["order_purchase_wday"] = base["order_purchase_timestamp"].dt.dayofweek

# --- Merge all ---
base = base.merge(custs[["customer_id", "customer_city", "customer_state"]], on="customer_id", how="left")
base = base.merge(items_agg, on="order_id", how="left")
base = base.merge(pays_agg, on="order_id", how="left")
if cat_per_order is not None:
    base = base.merge(cat_per_order, on="order_id", how="left")
base = base.merge(reviews, on="order_id", how="left")

# --- Final selection ---
cols = [
    "order_id", "customer_id", "review_score",
    "order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date",
    "delivery_delay_days", "order_purchase_date", "order_purchase_hour", "order_purchase_wday",
    "customer_city", "customer_state",
    "item_lines", "distinct_products", "distinct_sellers", "total_price", "total_freight",
    "total_payment", "primary_payment_type", "payment_types_count",
]
if "dominant_category" in base.columns:
    cols.append("dominant_category")

cols = [c for c in cols if c in base.columns]
final = base[cols].copy()

print("Rows:", len(final))
print("Null % by column:\n", (final.isna().mean() * 100).round(2).sort_values(ascending=False).head(10))

final.to_csv(OUTPUT_CSV, index=False)
print(f" Saved to {OUTPUT_CSV}")