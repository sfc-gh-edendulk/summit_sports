"""
Summit Sports - Social Media Ad Impressions Generator (Snowpark Stored Proc Style)

Generates realistic ad impression data for a one-week campaign in February 2025
(or any provided date window), including optional click events and per-impression cost.

Linkage to CRM:
- A daily, fresh random sample of CRM customers is attached to ~35% of impressions
  via the column CRM_CUSTOMER_ID (nullable). This enables ROAS-by-segment analysis
  by joining to SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM (and ultimately to sales).

Output table: SS_101.SOURCE_DATA.SOCIAL_AD_IMPRESSIONS
One row = one ad impression (optionally flagged as click).

Schema (uppercase for Snowflake):
  IMPRESSION_ID STRING
  PLATFORM STRING            -- e.g., Facebook, Instagram, TikTok, Google
  CAMPAIGN_ID STRING
  ADSET_ID STRING
  AD_ID STRING
  CREATIVE_ID STRING
  IMPRESSION_TIME TIMESTAMP
  DATE DATE
  HOUR NUMBER
  PLACEMENT STRING          -- e.g., Feed, Stories, Reels, Search
  DEVICE STRING             -- Mobile, Desktop
  COUNTRY STRING            -- 'FR'
  CITY STRING
  CPM_EUR FLOAT             -- CPM used for cost calc
  COST_EUR FLOAT            -- CPM/1000 per impression
  IS_CLICK BOOLEAN
  CLICK_TIME TIMESTAMP      -- nullable
  CRM_CUSTOMER_ID STRING    -- nullable, to link to CRM

Entrypoints:
- main(session): defaults to 2025-02-10 through 2025-02-16
- run(session, start_date, end_date, total_daily_impressions): parameterized
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import snowflake.snowpark as snowpark


WRITE_DB = "SS_101"
WRITE_SCHEMA = "SOURCE_DATA"
TARGET_TABLE = "SOCIAL_AD_IMPRESSIONS"


PLATFORMS: List[str] = ["Facebook", "Instagram", "TikTok", "Google"]
PLACEMENTS: Dict[str, List[str]] = {
    "Facebook": ["Feed", "Stories", "Reels"],
    "Instagram": ["Feed", "Stories", "Reels"],
    "TikTok": ["ForYou", "Stories"],
    "Google": ["Search", "Display", "YouTube"],
}
DEVICES = ["Mobile", "Desktop"]
CITIES_FR = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg",
    "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Saint-Étienne",
]

# CPM ranges per platform (EUR)
CPM_RANGE: Dict[str, Tuple[float, float]] = {
    "Facebook": (5.0, 9.0),
    "Instagram": (6.0, 10.0),
    "TikTok": (3.0, 6.0),
    "Google": (4.0, 8.0),
}

# CTR baselines per platform
CTR_BASE: Dict[str, float] = {
    "Facebook": 0.010,   # 1.0%
    "Instagram": 0.008,  # 0.8%
    "TikTok": 0.015,     # 1.5%
    "Google": 0.020,     # 2.0%
}

CRM_ATTACH_RATE = 0.35  # 35% of impressions get a CRM id
DAILY_CRM_POOL_CAP = 200_000


def _ensure_context(session: snowpark.Session) -> None:
    try:
        session.sql(f"USE DATABASE {WRITE_DB}").collect()
        session.sql(f"USE SCHEMA {WRITE_DB}.{WRITE_SCHEMA}").collect()
    except Exception:
        pass


def _ensure_target_table(session: snowpark.Session) -> None:
    _ensure_context(session)
    try:
        session.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {WRITE_DB}.{WRITE_SCHEMA}.{TARGET_TABLE} (
                IMPRESSION_ID STRING,
                PLATFORM STRING,
                CAMPAIGN_ID STRING,
                ADSET_ID STRING,
                AD_ID STRING,
                CREATIVE_ID STRING,
                IMPRESSION_TIME TIMESTAMP,
                DATE DATE,
                HOUR NUMBER,
                PLACEMENT STRING,
                DEVICE STRING,
                COUNTRY STRING,
                CITY STRING,
                CPM_EUR FLOAT,
                COST_EUR FLOAT,
                IS_CLICK BOOLEAN,
                CLICK_TIME TIMESTAMP,
                CRM_CUSTOMER_ID STRING
            )
            """
        ).collect()
    except Exception:
        pass


def _count_customers(session: snowpark.Session) -> int:
    try:
        pdf = session.sql(
            "SELECT COUNT(*) AS C FROM SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM"
        ).to_pandas()
        return int(pdf.iloc[0]["C"]) if not pdf.empty else 0
    except Exception:
        return 0


def _sample_customers_pool(session: snowpark.Session, sample_n: int) -> List[str]:
    if sample_n <= 0:
        return []
    try:
        return session.sql(
            f"""
            SELECT CUSTOMER_ID
            FROM SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM
            ORDER BY RANDOM()
            LIMIT {sample_n}
            """
        ).to_pandas()["CUSTOMER_ID"].astype(str).tolist()
    except Exception:
        return []


def _distribute_daily_impressions(total_daily: int) -> Dict[str, int]:
    # Simple weights per platform
    weights = {
        "Facebook": 0.35,
        "Instagram": 0.30,
        "TikTok": 0.20,
        "Google": 0.15,
    }
    counts = {p: int(total_daily * w) for p, w in weights.items()}
    # Adjust rounding remainder
    remainder = total_daily - sum(counts.values())
    for p in PLATFORMS:
        if remainder <= 0:
            break
        counts[p] += 1
        remainder -= 1
    return counts


def _generate_day(
    day: datetime,
    total_daily_impressions: int,
    crm_pool: List[str],
    rng: np.random.Generator,
) -> pd.DataFrame:
    by_platform = _distribute_daily_impressions(total_daily_impressions)
    rows: List[Dict[str, object]] = []

    for platform, n_impr in by_platform.items():
        if n_impr <= 0:
            continue
        cpm_low, cpm_high = CPM_RANGE[platform]
        ctr = CTR_BASE[platform]
        placements = PLACEMENTS[platform]

        # Synthesize ids for campaign hierarchy
        campaign_id = f"CAMP-{platform[:2].upper()}-{day.strftime('%Y%m%d')}"
        adset_id = f"SET-{rng.integers(10**6)}"

        for _ in range(n_impr):
            ad_id = f"AD-{rng.integers(10**7)}"
            creative_id = f"CR-{rng.integers(10**6)}"
            placement = rng.choice(placements)
            device = rng.choice(DEVICES, p=[0.8, 0.2])
            city = rng.choice(CITIES_FR)
            hour = int(rng.integers(0, 24))

            # Spread impressions across the hour
            minute = int(rng.integers(0, 60))
            second = int(rng.integers(0, 60))
            impression_ts = datetime(day.year, day.month, day.day, hour, minute, second)

            # Cost per impression from CPM
            cpm = float(rng.uniform(cpm_low, cpm_high))
            cost = round(cpm / 1000.0, 4)

            # Click determination
            is_click = bool(rng.random() < ctr)
            click_time = impression_ts + timedelta(seconds=int(rng.integers(0, 600))) if is_click else None

            # CRM link
            crm_id = None
            if crm_pool and (rng.random() < CRM_ATTACH_RATE):
                crm_id = rng.choice(crm_pool)

            rows.append({
                "IMPRESSION_ID": f"IMP-{rng.integers(10**9)}",
                "PLATFORM": platform,
                "CAMPAIGN_ID": campaign_id,
                "ADSET_ID": adset_id,
                "AD_ID": ad_id,
                "CREATIVE_ID": creative_id,
                "IMPRESSION_TIME": impression_ts,
                "DATE": impression_ts.date(),
                "HOUR": hour,
                "PLACEMENT": placement,
                "DEVICE": device,
                "COUNTRY": "FR",
                "CITY": city,
                "CPM_EUR": cpm,
                "COST_EUR": cost,
                "IS_CLICK": is_click,
                "CLICK_TIME": click_time,
                "CRM_CUSTOMER_ID": crm_id,
            })

    return pd.DataFrame(rows)


def _write_batch(session: snowpark.Session, df: pd.DataFrame, first_batch: bool) -> None:
    _ensure_context(session)
    if df.empty:
        return
    df.columns = df.columns.str.upper()
    session.write_pandas(
        df,
        TARGET_TABLE,
        database=WRITE_DB,
        schema=WRITE_SCHEMA,
        auto_create_table=True,
        overwrite=first_batch,
    )


def generate_ads(
    session: snowpark.Session,
    start_date: str = "2025-02-10",
    end_date: str = "2025-02-16",
    total_daily_impressions: int = 200_000,
) -> None:
    _ensure_context(session)
    _ensure_target_table(session)
    rng = np.random.default_rng(123)

    start = pd.to_datetime(start_date).date()
    end = pd.to_datetime(end_date).date()
    days = pd.date_range(start=start, end=end, freq="D")

    # We re-sample a fresh CRM pool per day (capped)
    customers_total = _count_customers(session)
    first_batch = True

    for day in days:
        pool_size = min(DAILY_CRM_POOL_CAP, customers_total)
        crm_pool = _sample_customers_pool(session, pool_size)
        day_df = _generate_day(pd.Timestamp(day).to_pydatetime(), total_daily_impressions, crm_pool, rng)
        _write_batch(session, day_df, first_batch)
        first_batch = False
        print(f"Wrote {len(day_df):,} impressions for {day}")


def main(session: snowpark.Session) -> snowpark.DataFrame:
    # Default week in Feb 2025
    generate_ads(session)
    return _return_sample(session)


def run(
    session: snowpark.Session,
    start_date: str = "2025-02-10",
    end_date: str = "2025-02-16",
    total_daily_impressions: int = 200_000,
) -> snowpark.DataFrame:
    generate_ads(session, start_date, end_date, total_daily_impressions)
    return _return_sample(session)


def _return_sample(session: snowpark.Session) -> snowpark.DataFrame:
    try:
        pdf = session.sql(
            f"SELECT * FROM {WRITE_DB}.{WRITE_SCHEMA}.{TARGET_TABLE} SAMPLE ROW (100)"
        ).to_pandas()
        return session.create_dataframe(pdf)
    except Exception:
        return session.create_dataframe([])


# No __main__ guard; designed for Snowpark stored procedure usage


