#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from pathlib import Path

# 🔁 1. Set your folder path (change this if needed)
data_dir = Path(r"C:\Users\saisa\Downloads\blackfridaysales")   # <- your folder

# 🔁 2. Read the 4 CSV files  ✅ FIXED VERSION
products   = pd.read_csv(data_dir / "bf_productsdata.csv",
                         sep=";", encoding="latin1")
stores     = pd.read_csv(data_dir / "bf_storesdata.csv",
                         sep=";", encoding="latin1")
promos     = pd.read_csv(data_dir / "bf_promotionsdata.csv",
                         sep=";", encoding="latin1")
orders_raw = pd.read_csv(data_dir / "bf_orders_rawdata.csv",
                         sep=";", encoding="latin1")

print("Rows in raw orders:", len(orders_raw))

# 🔁 3. Convert date column to proper datetime (bad ones → NaT)
orders = orders_raw.copy()
orders["order_datetime"] = pd.to_datetime(
    orders["order_datetime"],
    format="%Y-%m-%d %H:%M:%S",
    errors="coerce"   # invalid → NaT
)

# 🔁 4. Basic cleaning rules

# 4a. Drop rows with invalid datetime
before = len(orders)
orders = orders[orders["order_datetime"].notna()]
print("Removed invalid datetime rows:", before - len(orders))

# 4b. Keep only positive units
before = len(orders)
orders = orders[orders["units"] > 0]
print("Removed non-positive units:", before - len(orders))

# 4c. Clean discount column: allow 0–100 or NULL (for BOGO / missing)
# if column name is slightly different, adjust here
before = len(orders)
mask_discount_ok = (
    orders["discount_pct_applied"].between(0, 100)
    | orders["discount_pct_applied"].isna()
)
orders = orders[mask_discount_ok]
print("Removed invalid discounts:", before - len(orders))

# 4d. Keep only Completed orders (ignore Cancelled etc.)
if "order_status" in orders.columns:
    before = len(orders)
    orders = orders[orders["order_status"] == "Completed"]
    print("Removed non-completed orders:", before - len(orders))

# 🔁 5. Foreign key validation

# 5a. Only keep product_ids that exist in bf_products
valid_products = set(products["product_id"])
before = len(orders)
orders = orders[orders["product_id"].isin(valid_products)]
print("Removed rows with invalid product_id:", before - len(orders))

# 5b. Only keep store_ids that exist in bf_stores
valid_stores = set(stores["store_id"])
before = len(orders)
orders = orders[orders["store_id"].isin(valid_stores)]
print("Removed rows with invalid store_id:", before - len(orders))

# 5c. Promotion is OPTIONAL:
# if promotion_id not in bf_promotions → set it to NULL (NaN), don't drop the row
valid_promos = set(promos["promotion_id"].dropna())

mask_bad_promo = (
    orders["promotion_id"].notna() &
    ~orders["promotion_id"].isin(valid_promos)
)
print("Rows with invalid promotion_id (will be set to NULL):", mask_bad_promo.sum())

orders.loc[mask_bad_promo, "promotion_id"] = pd.NA

# 🔁 6. Drop exact duplicate rows if any
before = len(orders)
orders = orders.drop_duplicates()
print("Removed duplicate rows:", before - len(orders))

print("Final cleaned rows:", len(orders))

# 🔁 7. Save cleaned data
output_path = data_dir / "bf_orders_clean.csv"
orders.to_csv(output_path, index=False)
print("Saved cleaned orders to:", output_path)


# In[5]:


import pandas as pd
from pathlib import Path
import numpy as np
from datetime import datetime, timedelta

data_dir = Path(r"C:\Users\saisa\Downloads\blackfridaysales")

# 1. Load existing clean orders (from your previous cleaning step)
orders_2024 = pd.read_csv(data_dir / "bf_orders_clean.csv")

# 2. Load dimension tables (semicolon + latin1)
products = pd.read_csv(data_dir / "bf_productsdata.csv", sep=";", encoding="latin1")
stores   = pd.read_csv(data_dir / "bf_storesdata.csv", sep=";", encoding="latin1")
promos   = pd.read_csv(data_dir / "bf_promotionsdata.csv", sep=";", encoding="latin1")

print("Existing clean rows (2024):", len(orders_2024))

# 3. Helper: generate random datetimes between 2025-11-28 and 2025-11-30
def random_datetime(start_str="2025-11-28 00:00:00", end_str="2025-11-30 23:59:59", n=50):
    start = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    end   = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
    delta = end - start
    seconds = delta.days * 24 * 3600 + delta.seconds
    return [
        start + timedelta(seconds=np.random.randint(0, seconds+1))
        for _ in range(n)
    ]

# 4. Set how many 2025 orders you want
n_2025 = 50  # change to 100 if you want a bigger dataset

np.random.seed(42)

product_ids   = products["product_id"].tolist()
store_ids     = stores["store_id"].tolist()
promo_ids     = promos["promotion_id"].dropna().tolist()
channels      = ["Online", "In-Store"]
payments      = ["Card", "Cash", "Wallet"]

# For price calculation, we need regular prices and discount_pct
product_price_map = products.set_index("product_id")["regular_price"].to_dict()
promo_type_map    = promos.set_index("promotion_id")["promo_type"].to_dict()
promo_disc_map    = promos.set_index("promotion_id")["discount_pct"].to_dict()

new_rows = []

dates_2025 = random_datetime(n=n_2025)

for i in range(n_2025):
    order_id = f"BF2025-{1000 + i:04d}"
    order_datetime = dates_2025[i]
    store_id = np.random.choice(store_ids)
    product_id = np.random.choice(product_ids)

    # some orders have no promotion
    if np.random.rand() < 0.2:
        promotion_id = pd.NA
        discount_applied = np.nan
    else:
        promotion_id = np.random.choice(promo_ids)
        promo_type = promo_type_map.get(promotion_id, "Percentage")
        promo_disc = promo_disc_map.get(promotion_id)

        if promo_type == "Percentage" and pd.notna(promo_disc):
            discount_applied = float(promo_disc)
        elif promo_type in ["Flash Deal", "Coupon"] and pd.notna(promo_disc):
            discount_applied = float(promo_disc)
        else:
            # BOGO / others → treat discount as 0 for column, price logic handled via units
            discount_applied = 0.0

    channel = np.random.choice(channels)
    payment_method = np.random.choice(payments)
    units = int(np.random.choice([1, 1, 1, 2, 2, 3]))  # more 1–2 unit orders

    regular_price = product_price_map.get(product_id, 100.0)

    # simple final price rule
    if pd.notna(promotion_id) and pd.notna(discount_applied) and discount_applied > 0:
        unit_price = round(regular_price * (1 - discount_applied / 100), 2)
    else:
        # no promo or BOGO-type: small random discount 0–15%
        rand_disc = np.random.uniform(0, 0.15)
        unit_price = round(regular_price * (1 - rand_disc), 2)

    new_rows.append({
        "order_id": order_id,
        "order_datetime": order_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "store_id": store_id,
        "product_id": product_id,
        "promotion_id": promotion_id,
        "channel": channel,
        "payment_method": payment_method,
        "units": units,
        "unit_selling_price": unit_price,
        "discount_pct_applied": discount_applied,
        "order_status": "Completed"
    })

orders_2025 = pd.DataFrame(new_rows)
print("Generated 2025 rows:", len(orders_2025))

# 5. Combine 2024 + 2025
orders_all = pd.concat([orders_2024, orders_2025], ignore_index=True)

print("Total rows after adding 2025:", len(orders_all))

# 6. Save as new clean file for Power BI
output_path = data_dir / "bf_orders_clean_all.csv"
orders_all.to_csv(output_path, index=False)
print("Saved combined clean orders to:", output_path)


# In[ ]:




