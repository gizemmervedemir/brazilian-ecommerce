#DATA ANALYSIS
#Starting data exploration

import numpy as np
import pandas as pd

# 1. reading the sheet that contains raw data from the CSV file
df = pd.read_csv("/Users/gizemmervedemir/ACM476/data_ready_for_cs.csv")

# 2. displaying the first 5 rows for verification
print(df.head())

print("Data shape:", df.shape)
print(df.dtypes)

# 3. Quick ID checks
print("\nUnique order_id:", df["order_id"].nunique(), "/", df.shape[0])
print("Unique customer_id:", df["customer_id"].nunique(), "/", df.shape[0])

# 4. Convert date columns to datetime
date_cols = [
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "order_purchase_date"
]

for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors="coerce")

print("\nDatetime conversion check:")
print(df[date_cols].dtypes)

# 5. Drop ID columns (becuase they're not analytical features)
id_cols = ["order_id", "customer_id"]
df_model = df.drop(columns=id_cols)

# 6. Separate variable types
num_cols = df_model.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df_model.select_dtypes(exclude=[np.number, 'datetime64[ns]']).columns.tolist()
time_cols = df_model.select_dtypes(include=['datetime64[ns]']).columns.tolist()

print("Numeric columns:", num_cols)
print("Categorical columns:", cat_cols[:10], "...")
print("Time columns:", time_cols)

print(df[date_cols].head())


# 7. Feature engineering from timestamps
df["purchase_year"]  = df["order_purchase_timestamp"].dt.year
df["purchase_month"] = df["order_purchase_timestamp"].dt.month
df["purchase_wday"]  = df["order_purchase_timestamp"].dt.dayofweek  # 0=Monday
df["is_weekend"]     = df["purchase_wday"].isin([5, 6]).astype(int)
df["purchase_hour"]  = df["order_purchase_timestamp"].dt.hour

# 8. Delivery metrics
df["delivery_time_days"] = (
    df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
).dt.days

df["estimated_delay_days"] = (
    df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]
).dt.days  # (+) late, (-) early

print("\nNew time-based features added successfully.")

# 9. Descriptive statistics (numeric only)
desc_agg = ['sum', 'mean', 'std', 'var', 'min', 'max']
desc_agg_dict = {col: desc_agg for col in num_cols}

desc_summ = df[num_cols].agg(desc_agg_dict).T  # transpose → variables as rows
#printing desc_summ to examine each variable's sum, mean, standard deviation, min and max values
print("\nDescriptive summary for numeric columns:")
print(desc_summ.head())

# 10. Convert summary & df to numpy arrays
df_desc_na = desc_summ.to_numpy()

#to use df as a numpy array; for vector operations, etc.
df_na = df[num_cols].to_numpy()

print("\nNumeric summary array shape:", df_desc_na.shape)
print("Numeric df array shape:", df_na.shape)

#Continuing Overview
# 11. Quick shape & missing info overview
print("\nFinal shape:", df.shape)
print("\nMissing value ratio per column:")
print(df.isna().mean().round(3))

import matplotlib.pyplot as plt
import seaborn as sns

# Delivery time distribution
sns.histplot(df["delivery_time_days"], bins=50, kde=True)
plt.title("Delivery Time Distribution")

# Delay analysis
sns.histplot(df["estimated_delay_days"], bins=50, kde=True)
plt.title("Estimated Delay (Days)")

# Satisfaction relationship
sns.boxplot(x="review_score", y="delivery_time_days", data=df)
plt.title("Delivery Time vs Review Score")