"""
Summit Sports Sales Generator - Snowpark Stored Procedure Style

Generates synthetic B2C sales data for Summit Sports with:
- Annual revenue targets equal to 35% of reference Intersport totals
  (2021: 2.76B€, 2022: 3.26B€, 2023: 3.60B€, 2024: 3.88B€)
- Average basket value around ~100€ with large variance (lognormal)
- Daily targets indexed to Carrefour stock price history from stage CSV
  (missing days are interpolated to a daily time series)
- 60% of orders linked to CRM customer ids
- Writes to SS_101.SOURCE_DATA.SUMMIT_SPORTS_SALES in monthly batches

Inputs in Snowflake:
- Products: SS_101.SOURCE_DATA.SS_PRODUCTS (PRODUCT_ID)
- Stores:   SS_101.RAW_POS.SS_STORES (STOREID)
- CRM:      SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM (CUSTOMER_ID)
- Stage CSV: @SS_101.SOURCE_DATA.SOURCE_DATA_STAGE/Carrefour Stock Price History.csv

Entrypoints:
- main(session)  -> full 2021-2024 generation
- run(session, start_year, end_year) -> parameterized range

Returns: A small sample Snowpark DataFrame of generated sales (up to 100 rows)
"""

from __future__ import annotations

import math
import random
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType


# ----------------------------- Configuration -----------------------------

WRITE_DB = "SS_101"
WRITE_SCHEMA = "SOURCE_DATA"
TARGET_TABLE = "SUMMIT_SPORTS_SALES"

# Reference Intersport annual totals (EUR). We'll generate 35% of these
REFERENCE_ANNUALS: Dict[int, float] = {
    2021: 2.76e9,
    2022: 3.26e9,
    2023: 3.60e9,
    2024: 3.88e9,
}
TARGET_SHARE = 0.35

# Average order value and variability
AOV_TARGET_EUR = 100.0
LOGNORMAL_SIGMA = 0.55  # larger variance

# Order composition
MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 5
CUSTOMER_ATTACH_RATE = 0.60  # 60% of orders have customer id
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Gift Card", "Cash"]

# Batching
WRITE_BATCH_BY = "month"  # Generate and write per month


# ------------------------------- Utilities -------------------------------

def _ensure_context(session: snowpark.Session) -> None:
    try:
        session.sql(f"USE DATABASE {WRITE_DB}").collect()
        session.sql(f"USE SCHEMA {WRITE_DB}.{WRITE_SCHEMA}").collect()
    except Exception:
        pass


def _return_schema() -> StructType:
    return StructType([
        StructField("ORDER_ID", StringType()),
        StructField("STOREID", StringType()),
        StructField("SALE_DATE", DateType()),
        StructField("PRODUCT_ID", StringType()),
        StructField("QUANTITY", IntegerType()),
        StructField("SALES_PRICE_EURO", DoubleType()),
        StructField("PAYMENT_METHOD", StringType()),
        StructField("CUSTOMER_ID", StringType()),
    ])


def _read_stock_index(session: snowpark.Session, start_date: str, end_date: str) -> pd.DataFrame:
    """Read Carrefour stock CSV from stage; return daily index between start/end with interpolation.
    Expected CSV columns include Date and Close (header row present). We'll select by position.
    """
    # Attempt to read CSV from stage
    try:
        query = f"""
            SELECT 
                TRY_TO_DATE($1) AS DATE,
                TRY_TO_DOUBLE($5) AS CLOSE
            FROM @SS_101.SOURCE_DATA.SOURCE_DATA_STAGE/""" + "Carrefour Stock Price History.csv" + """
            (FILE_FORMAT => (TYPE => 'CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"'))
        """
        df = session.sql(query).to_pandas()
        df = df.dropna(subset=["DATE"]).sort_values("DATE")
        df = df[(df["DATE"] >= pd.to_datetime(start_date)) & (df["DATE"] <= pd.to_datetime(end_date))]
        # If CLOSE missing, forward fill
        df["CLOSE"] = df["CLOSE"].fillna(method="ffill")
    except Exception:
        # Fallback: flat index
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        return pd.DataFrame({"DATE": dates, "INDEX": np.ones(len(dates))})

    # Build daily date range and interpolate missing days
    full = pd.DataFrame({"DATE": pd.date_range(start=start_date, end=end_date, freq="D")})
    merged = full.merge(df[["DATE", "CLOSE"]], on="DATE", how="left")
    merged["CLOSE"] = merged["CLOSE"].interpolate(method="linear").fillna(method="bfill").fillna(method="ffill")

    # Normalize to positive weights, then add a modest seasonal curve (optional)
    x = merged["CLOSE"].values.astype(float)
    # Normalize to mean ~1.0
    x_norm = x / np.maximum(np.mean(x), 1e-9)
    merged["INDEX"] = np.clip(x_norm, 0.2, 5.0)
    return merged[["DATE", "INDEX"]]


def _year_targets() -> Dict[int, float]:
    return {y: TARGET_SHARE * v for y, v in REFERENCE_ANNUALS.items()}


def _build_daily_targets(stock_idx: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
    """Compute daily revenue targets by distributing annual targets by daily index weights per year."""
    targets = _year_targets()
    df = stock_idx.copy()
    df["YEAR"] = pd.to_datetime(df["DATE"]).dt.year
    df = df[(df["YEAR"] >= start_year) & (df["YEAR"] <= end_year)]
    daily_targets = []
    for year, group in df.groupby("YEAR"):
        if year not in targets:
            continue
        weights = group["INDEX"].values.astype(float)
        weights_sum = np.sum(weights)
        if weights_sum <= 0:
            weights = np.ones_like(weights)
            weights_sum = np.sum(weights)
        year_target = float(targets[year])
        alloc = year_target * (weights / weights_sum)
        tmp = group[["DATE"]].copy()
        tmp["TARGET_EUR"] = alloc
        daily_targets.append(tmp)
    if not daily_targets:
        return pd.DataFrame(columns=["DATE", "TARGET_EUR"]).astype({"DATE": "datetime64[ns]"})
    out = pd.concat(daily_targets, ignore_index=True)
    out["DATE"] = pd.to_datetime(out["DATE"]).dt.date
    return out


def _fetch_dimension_lists(session: snowpark.Session) -> Tuple[List[str], List[str], List[str]]:
    # Products
    products = session.sql("SELECT PRODUCT_ID FROM SS_101.SOURCE_DATA.SS_PRODUCTS").to_pandas()["PRODUCT_ID"].astype(str).tolist()
    # Stores
    stores = session.sql("SELECT STOREID FROM SS_101.RAW_POS.SS_STORES").to_pandas()["STOREID"].astype(str).tolist()
    # Customers
    try:
        customers = session.sql("SELECT CUSTOMER_ID FROM SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM").to_pandas()["CUSTOMER_ID"].astype(str).tolist()
    except Exception:
        customers = []
    return products, stores, customers


def _sample_aov(size: int, rng: np.random.Generator) -> np.ndarray:
    # For lognormal, mean = exp(mu + 0.5*sigma^2). Solve mu for target mean.
    sigma = LOGNORMAL_SIGMA
    mu = math.log(max(AOV_TARGET_EUR, 1.0)) - 0.5 * sigma * sigma
    return rng.lognormal(mean=mu, sigma=sigma, size=size)


def _generate_day_orders(
    day: pd.Timestamp,
    day_target_eur: float,
    stores: List[str],
    products: List[str],
    customers: List[str],
    rng: np.random.Generator,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if day_target_eur <= 0 or not stores or not products:
        return rows

    # Allocate target evenly across stores (could be weighted by store capacity)
    store_target = day_target_eur / max(len(stores), 1)

    for store_id in stores:
        # Estimate number of orders for this store ~= target / AOV, then sample variability
        expected_orders = max(int(store_target / max(AOV_TARGET_EUR, 1.0)), 1)
        num_orders = max(int(rng.normal(loc=expected_orders, scale=max(1, expected_orders * 0.3))), 1)
        order_totals = _sample_aov(num_orders, rng)

        for order_total in order_totals:
            # Build one order
            order_id = f"ORD-{day.strftime('%Y%m%d')}-{rng.integers(10**9)}"
            payment_method = rng.choice(PAYMENT_METHODS)
            attach_customer = rng.random() < CUSTOMER_ATTACH_RATE and len(customers) > 0
            customer_id = rng.choice(customers) if attach_customer else None

            num_items = int(rng.integers(MIN_ITEMS_PER_ORDER, MAX_ITEMS_PER_ORDER + 1))
            # Split basket total across items using a Dirichlet; ensure positive amounts
            weights = rng.dirichlet(np.ones(num_items))
            item_prices = np.maximum(order_total * weights, 0.5)

            for k in range(num_items):
                product_id = rng.choice(products)
                rows.append({
                    "ORDER_ID": order_id,
                    "STOREID": store_id,
                    "SALE_DATE": day.date(),
                    "PRODUCT_ID": product_id,
                    "QUANTITY": 1,
                    "SALES_PRICE_EURO": float(round(item_prices[k], 2)),
                    "PAYMENT_METHOD": payment_method,
                    "CUSTOMER_ID": customer_id,
                })

    return rows


def _write_batch(session: snowpark.Session, df: pd.DataFrame, first_batch: bool) -> None:
    _ensure_context(session)
    # Ensure uppercase columns
    df.columns = df.columns.str.upper()
    session.write_pandas(
        df,
        TARGET_TABLE,
        database=WRITE_DB,
        schema=WRITE_SCHEMA,
        auto_create_table=True,
        overwrite=first_batch,
    )


def _month_iter(start_year: int, end_year: int) -> List[Tuple[int, int]]:
    months: List[Tuple[int, int]] = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            months.append((y, m))
    return months


def generate_sales(session: snowpark.Session, start_year: int, end_year: int) -> None:
    _ensure_context(session)
    rng = np.random.default_rng(42)

    # Build stock-based daily targets
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    stock = _read_stock_index(session, start_date, end_date)
    daily_targets = _build_daily_targets(stock, start_year, end_year)

    products, stores, customers = _fetch_dimension_lists(session)

    first_batch = True
    for year, month in _month_iter(start_year, end_year):
        # Filter days in this month
        mask = (
            (pd.to_datetime(daily_targets["DATE"]).dt.year == year) &
            (pd.to_datetime(daily_targets["DATE"]).dt.month == month)
        )
        month_days = daily_targets.loc[mask]
        if month_days.empty:
            continue

        month_rows: List[Dict[str, object]] = []
        for _, row in month_days.iterrows():
            day = pd.to_datetime(row["DATE"])  # Timestamp
            target = float(row["TARGET_EUR"])
            month_rows.extend(
                _generate_day_orders(day, target, stores, products, customers, rng)
            )

        if not month_rows:
            continue

        month_df = pd.DataFrame(month_rows)
        _write_batch(session, month_df, first_batch)
        first_batch = False
        print(f"Wrote {len(month_df):,} rows for {year}-{month:02d}")


def main(session: snowpark.Session) -> snowpark.DataFrame:
    _ensure_context(session)
    print("Starting Summit Sports Sales generation (2021-2024)...")
    generate_sales(session, 2021, 2024)
    return _return_sample(session)


def run(session: snowpark.Session, start_year: int = 2021, end_year: int = 2024) -> snowpark.DataFrame:
    _ensure_context(session)
    print(f"Starting Summit Sports Sales generation ({start_year}-{end_year})...")
    generate_sales(session, start_year, end_year)
    return _return_sample(session)


def _return_sample(session: snowpark.Session) -> snowpark.DataFrame:
    _ensure_context(session)
    schema = _return_schema()
    try:
        # Return a small sample from the target table
        pdf = session.sql(
            f"SELECT * FROM {WRITE_DB}.{WRITE_SCHEMA}.{TARGET_TABLE} SAMPLE ROW (100)"
        ).to_pandas()
        # Align columns to schema
        for col in [f.name for f in schema.fields]:
            if col not in pdf.columns:
                pdf[col] = None
        pdf = pdf[[f.name for f in schema.fields]]
        return session.create_dataframe(pdf, schema=schema)
    except Exception:
        # Return empty if table absent
        return session.create_dataframe([], schema=schema)


# Note: no __main__ guard; designed for Snowflake Snowpark stored procedure handler usage.


