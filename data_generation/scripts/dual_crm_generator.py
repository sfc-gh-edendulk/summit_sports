"""
Dual CRM Generator for Summit Sports Data Clean Room Scenarios

Creates two synthetic CRM datasets with controlled overlaps and realistic
data quality issues. Datasets are written to SS_101.SOURCE_DATA:

- SS_101.SOURCE_DATA.CROCEVIA_CRM (target ~6,000,000 rows)
- SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM (target ~2,000,000 rows)

Key characteristics:
- Person attributes (GENDER, FIRST_NAME, LAST_NAME, BIRTH_DATE) sourced from
  SS_101.SOURCE_DATA.FRENCH_PEOPLE
- Address attributes (NUMERO, NOM_VOIE, CODE_POSTAL, LON, LAT) sourced from
  SS_101.SOURCE_DATA.ADRESSES_FRANCE
- French phone numbers, ~90% unique overall, with some duplicates to simulate
  multiple profiles; a small portion intentionally overlaps across datasets
- Overlap between datasets: at least 60% of SUMMIT_SPORTS_CRM rows overlap with
  CROCEVIA_CRM across 1..N fields (name, birth date, phone, address, email)
- Realistic "messiness": missing values across email/phone/postal_code/birthdate

Usage pattern mirrors other generators: provide a Snowpark Session and call main(session).
This script is designed for execution as a Snowpark handler or a standalone tool where
the host application constructs and passes a Snowpark Session.
"""

from __future__ import annotations

import math
import random
import string
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional

import numpy as np
import pandas as pd
import snowflake.snowpark as snowpark


# ----------------------------- Configuration -----------------------------

TARGET_ROWS_CROCEVIA: int = 6_000_000
TARGET_ROWS_SUMMIT: int = 2_000_000
WRITE_SCHEMA: str = "SS_101.SOURCE_DATA"
CROCEVIA_TABLE: str = f"{WRITE_SCHEMA}.CROCEVIA_CRM"
SUMMIT_TABLE: str = f"{WRITE_SCHEMA}.SUMMIT_SPORTS_CRM"

# Batch sizes should balance memory footprint and Snowflake load times
BATCH_SIZE_CROCEVIA: int = 200_000
BATCH_SIZE_SUMMIT: int = 200_000

# Missingness probabilities (tuned for realism)
MISSING_EMAIL_P: float = 0.15
MISSING_PHONE_P: float = 0.20
MISSING_POSTAL_P: float = 0.10
MISSING_BIRTHDATE_P: float = 0.07

# Duplicate profiles ratio (extra rows created as minor variations)
DUPLICATE_PROFILE_RATIO: float = 0.03

# Summit overlap target
SUMMIT_OVERLAP_RATIO: float = 0.60  # >= 60% of smaller set must overlap

FRENCH_EMAIL_DOMAINS: List[str] = [
    "gmail.com", "orange.fr", "free.fr", "wanadoo.fr", "sfr.fr", "laposte.net"
]


# ------------------------------ Data Models ------------------------------

@dataclass
class OverlapPlan:
    """Defines proportions for overlap field combinations for Summit vs Crocevia."""
    all_fields: float = 0.10
    three_fields: float = 0.20
    two_fields: float = 0.20
    one_field: float = 0.10
    # Remaining up to SUMMIT_OVERLAP_RATIO are counted by the above; any residue treated as unique

    def normalized(self) -> "OverlapPlan":
        total = self.all_fields + self.three_fields + self.two_fields + self.one_field
        if total == 0:
            return self
        return OverlapPlan(
            all_fields=self.all_fields / total * SUMMIT_OVERLAP_RATIO,
            three_fields=self.three_fields / total * SUMMIT_OVERLAP_RATIO,
            two_fields=self.two_fields / total * SUMMIT_OVERLAP_RATIO,
            one_field=self.one_field / total * SUMMIT_OVERLAP_RATIO,
        )


# ------------------------------ Utilities --------------------------------

def _safe_choice(seq: List, size: int) -> List:
    if not seq:
        return []
    if size <= len(seq):
        return random.sample(seq, size)
    # If fewer unique than needed, sample with replacement
    return random.choices(seq, k=size)


def _sanitize_for_email(local: str) -> str:
    local = local.lower()
    local = local.replace("'", "").replace("ç", "c").replace("é", "e").replace("è", "e").replace("ë", "e")
    local = local.replace("à", "a").replace("â", "a").replace("î", "i").replace("ï", "i").replace("ô", "o").replace("û", "u")
    allowed = string.ascii_lowercase + string.digits + ".-_"
    return "".join(ch for ch in local if ch in allowed)


def _generate_emails(first_names: List[str], last_names: List[str]) -> List[str]:
    emails: List[str] = []
    for fn, ln in zip(first_names, last_names):
        if random.random() < 0.7:
            local = _sanitize_for_email(f"{fn}.{ln}")
            emails.append(f"{local}@{random.choice(FRENCH_EMAIL_DOMAINS)}")
        else:
            # random handle
            handle = _sanitize_for_email(f"{fn}{ln}{random.randint(1, 9999)}")
            emails.append(f"{handle}@{random.choice(FRENCH_EMAIL_DOMAINS)}")
    return emails


def _generate_french_phones(n: int, unique_ratio: float = 0.90) -> List[str]:
    """Generate French phone numbers as strings. 90% unique by default."""
    unique_count = int(n * unique_ratio)
    unique_set = set()
    phones: List[str] = []

    def make_phone() -> str:
        # Format: 0X XX XX XX XX with realistic starting digits
        first_digit = random.choice([6, 7, 1, 2, 3, 4, 5, 9])  # include mobile (6,7) and landlines
        digits = ["0", str(first_digit)] + [str(random.randint(0, 9)) for _ in range(8)]
        # group as 0X XX XX XX XX
        return f"{digits[0]}{digits[1]} {digits[2]}{digits[3]} {digits[4]}{digits[5]} {digits[6]}{digits[7]} {digits[8]}{digits[9]}"

    while len(unique_set) < unique_count:
        unique_set.add(make_phone())

    unique_list = list(unique_set)
    phones.extend(unique_list)
    remaining = n - len(phones)
    if remaining > 0:
        phones.extend(random.choices(unique_list, k=remaining))
    random.shuffle(phones)
    return phones


def _inject_missingness(df: pd.DataFrame) -> pd.DataFrame:
    # Missing email
    mask = np.random.rand(len(df)) < MISSING_EMAIL_P
    df.loc[mask, "EMAIL"] = None

    # Missing phone
    mask = np.random.rand(len(df)) < MISSING_PHONE_P
    df.loc[mask, "PHONE"] = None

    # Missing postal code
    if "POSTAL_CODE" in df.columns:
        mask = np.random.rand(len(df)) < MISSING_POSTAL_P
        df.loc[mask, "POSTAL_CODE"] = None

    # Missing birth date
    if "BIRTH_DATE" in df.columns:
        mask = np.random.rand(len(df)) < MISSING_BIRTHDATE_P
        df.loc[mask, "BIRTH_DATE"] = None

    return df


def _add_duplicate_profiles(df: pd.DataFrame, ratio: float) -> pd.DataFrame:
    """Add duplicate-like profiles with minor variations to simulate multiple profiles."""
    count = int(len(df) * ratio)
    if count <= 0:
        return df
    idxs = np.random.choice(len(df), size=count, replace=False)
    dupes = df.iloc[idxs].copy()
    # Modify minor fields to simulate real-world variations
    # - sometimes tweak email local part
    for i in range(len(dupes)):
        if pd.notna(dupes.iloc[i]["EMAIL"]) and random.random() < 0.5:
            email = str(dupes.iloc[i]["EMAIL"])  # type: ignore[index]
            parts = email.split("@")
            if len(parts) == 2:
                dupes.iat[i, dupes.columns.get_loc("EMAIL")] = f"{parts[0]}{random.randint(0,9)}@{parts[1]}"
        # sometimes tweak last digit of phone
        if pd.notna(dupes.iloc[i]["PHONE"]) and random.random() < 0.5:
            phone = str(dupes.iloc[i]["PHONE"])  # type: ignore[index]
            digits = [c for c in phone if c.isdigit()]
            if digits:
                digits[-1] = str(random.randint(0, 9))
                # reconstruct in 0X XX XX XX XX
                try:
                    new_phone = f"{digits[0]}{digits[1]} {digits[2]}{digits[3]} {digits[4]}{digits[5]} {digits[6]}{digits[7]} {digits[8]}{digits[9]}"
                    dupes.iat[i, dupes.columns.get_loc("PHONE")] = new_phone
                except Exception:
                    pass
        # maybe blank postal code or street
        if "POSTAL_CODE" in dupes.columns and random.random() < 0.2:
            dupes.iat[i, dupes.columns.get_loc("POSTAL_CODE")] = None
        if "STREET" in dupes.columns and random.random() < 0.1:
            dupes.iat[i, dupes.columns.get_loc("STREET")] = None
    # Give new customer ids to duplicates
    if "CUSTOMER_ID" in dupes.columns:
        suffix = np.random.randint(0, 9, size=len(dupes))
        dupes["CUSTOMER_ID"] = dupes["CUSTOMER_ID"].astype(str) + "_DUP" + suffix.astype(str)
    return pd.concat([df, dupes], ignore_index=True)


# ---------------------------- Data Loaders -------------------------------

def load_people(session: snowpark.Session, count: int) -> pd.DataFrame:
    query = f"""
        SELECT GENDER, FIRST_NAME, LAST_NAME, BIRTH_DATE
        FROM SS_101.SOURCE_DATA.FRENCH_PEOPLE
        ORDER BY RANDOM()
        LIMIT {count}
    """
    df = session.sql(query).to_pandas()
    # Ensure correct dtypes
    for col in ["GENDER", "FIRST_NAME", "LAST_NAME"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    # BIRTH_DATE remains as is (date-like)
    return df


def load_addresses(session: snowpark.Session, count: int) -> pd.DataFrame:
    query = f"""
        SELECT NUMERO, NOM_VOIE, CODE_POSTAL, LON, LAT
        FROM SS_101.SOURCE_DATA.ADRESSES_FRANCE
        WHERE CODE_POSTAL IS NOT NULL
        ORDER BY RANDOM()
        LIMIT {count}
    """
    adf = session.sql(query).to_pandas()
    adf["NUMERO"] = adf["NUMERO"].astype(str).replace({"nan": None})
    adf["NOM_VOIE"] = adf["NOM_VOIE"].astype(str).replace({"nan": None})
    # Compose street string
    def compose_street(num, voie):
        if num and num != "None" and voie and voie != "None":
            return f"{num} {voie}"
        return voie if voie and voie != "None" else None
    adf["STREET"] = [compose_street(n, v) for n, v in zip(adf["NUMERO"], adf["NOM_VOIE"])]
    adf.rename(columns={"CODE_POSTAL": "POSTAL_CODE", "LON": "LONGITUDE", "LAT": "LATITUDE"}, inplace=True)
    keep_cols = ["STREET", "POSTAL_CODE", "LATITUDE", "LONGITUDE"]
    return adf[keep_cols]


# --------------------------- Core Generators -----------------------------

def _build_base_batch(
    people_df: pd.DataFrame,
    addr_df: pd.DataFrame,
    source_name: str,
    start_index: int,
    total_needed: int,
) -> pd.DataFrame:
    size = len(people_df)
    # Match addresses to a fraction of people
    addr_take = min(len(addr_df), size)
    # pad addresses if fewer than people
    if addr_take < size:
        pad = _safe_choice(addr_df.to_dict("records"), size - addr_take)
        addr_records = addr_df.to_dict("records") + pad
    else:
        addr_records = addr_df.sample(n=size).to_dict("records")

    first_names = people_df["FIRST_NAME"].astype(str).tolist()
    last_names = people_df["LAST_NAME"].astype(str).tolist()
    birth_dates = people_df["BIRTH_DATE"].tolist()
    genders = people_df["GENDER"].astype(str).tolist()
    emails = _generate_emails(first_names, last_names)
    phones = _generate_french_phones(size, unique_ratio=0.90)

    rows = []
    for i in range(size):
        addr = addr_records[i] if i < len(addr_records) else {"STREET": None, "POSTAL_CODE": None, "LATITUDE": None, "LONGITUDE": None}
        rows.append({
            "CUSTOMER_ID": f"{source_name[:3].upper()}-{start_index + i:010d}",
            "FIRST_NAME": first_names[i],
            "LAST_NAME": last_names[i],
            "GENDER": genders[i],
            "BIRTH_DATE": birth_dates[i],
            "EMAIL": emails[i],
            "PHONE": phones[i],
            "STREET": addr.get("STREET"),
            "POSTAL_CODE": str(addr.get("POSTAL_CODE")) if pd.notna(addr.get("POSTAL_CODE")) else None,
            "LATITUDE": float(addr.get("LATITUDE")) if pd.notna(addr.get("LATITUDE")) else None,
            "LONGITUDE": float(addr.get("LONGITUDE")) if pd.notna(addr.get("LONGITUDE")) else None,
            "SOURCE": source_name,
            "OVERLAP_TYPE": "NONE",
        })

    df = pd.DataFrame(rows)
    df = _inject_missingness(df)
    df = _add_duplicate_profiles(df, DUPLICATE_PROFILE_RATIO)
    return df


def _apply_overlap_to_summit_batch(
    summit_df: pd.DataFrame,
    crocevia_pool_df: pd.DataFrame,
    overlap_ratio: float,
    plan: OverlapPlan,
) -> pd.DataFrame:
    if len(summit_df) == 0 or len(crocevia_pool_df) == 0:
        return summit_df

    plan = plan.normalized()
    n = len(summit_df)
    target_overlap = int(n * overlap_ratio)
    if target_overlap <= 0:
        return summit_df

    indices = np.random.permutation(n)[:target_overlap]
    remaining_indices = list(indices)

    # Compute counts per overlap category
    c_all = int(target_overlap * plan.all_fields)
    c_three = int(target_overlap * plan.three_fields)
    c_two = int(target_overlap * plan.two_fields)
    c_one = int(target_overlap * plan.one_field)
    # Distribute any remainder to one_field
    used = c_all + c_three + c_two + c_one
    c_one += (target_overlap - used)

    # Helper to copy fields from a crocevia record to a summit record
    def copy_fields(dst_idx: int, src_row: pd.Series, fields: List[str]):
        for f in fields:
            summit_df.at[dst_idx, f] = src_row.get(f)
        summit_df.at[dst_idx, "OVERLAP_TYPE"] = "+".join(sorted(fields)) if fields else "NONE"

    # Sample source rows for overlaps
    src_sample = crocevia_pool_df.sample(n=target_overlap, replace=True).reset_index(drop=True)

    ptr = 0
    # All 4 core fields: FIRST_NAME, LAST_NAME, BIRTH_DATE, PHONE (and we include EMAIL too)
    for _ in range(c_all):
        if not remaining_indices:
            break
        idx = remaining_indices.pop()
        copy_fields(idx, src_sample.iloc[ptr], [
            "FIRST_NAME", "LAST_NAME", "BIRTH_DATE", "PHONE", "EMAIL", "STREET", "POSTAL_CODE"
        ])
        ptr += 1

    # Three-field overlaps (random combos)
    triplets = [
        ["FIRST_NAME", "LAST_NAME", "BIRTH_DATE"],
        ["FIRST_NAME", "LAST_NAME", "PHONE"],
        ["FIRST_NAME", "BIRTH_DATE", "PHONE"],
        ["LAST_NAME", "BIRTH_DATE", "PHONE"],
    ]
    for _ in range(c_three):
        if not remaining_indices:
            break
        idx = remaining_indices.pop()
        fields = random.choice(triplets)
        copy_fields(idx, src_sample.iloc[ptr], fields)
        ptr += 1

    # Two-field overlaps
    pairs = [
        ["FIRST_NAME", "LAST_NAME"],
        ["FIRST_NAME", "BIRTH_DATE"],
        ["FIRST_NAME", "PHONE"],
        ["LAST_NAME", "BIRTH_DATE"],
        ["LAST_NAME", "PHONE"],
        ["BIRTH_DATE", "PHONE"],
    ]
    for _ in range(c_two):
        if not remaining_indices:
            break
        idx = remaining_indices.pop()
        fields = random.choice(pairs)
        copy_fields(idx, src_sample.iloc[ptr], fields)
        ptr += 1

    # One-field overlaps
    singles = ["FIRST_NAME", "LAST_NAME", "BIRTH_DATE", "PHONE", "EMAIL", "POSTAL_CODE"]
    for _ in range(c_one):
        if not remaining_indices:
            break
        idx = remaining_indices.pop()
        field = random.choice(singles)
        copy_fields(idx, src_sample.iloc[ptr], [field])
        ptr += 1

    return summit_df


def _write_batch(
    session: snowpark.Session,
    df: pd.DataFrame,
    table_fqn: str,
    first_batch: bool,
):
    # Ensure uppercase columns for Snowflake compatibility
    df.columns = df.columns.str.upper()
    session.write_pandas(
        df,
        table_fqn,
        auto_create_table=True,
        overwrite=first_batch,
    )


# ------------------------------ Orchestration ----------------------------

def generate_crocevia(session: snowpark.Session, total_rows: int) -> None:
    first_batch = True
    generated = 0
    start_index = 0
    while generated < total_rows:
        batch = min(BATCH_SIZE_CROCEVIA, total_rows - generated)
        people = load_people(session, batch)
        addresses = load_addresses(session, int(batch * 0.6))  # addresses for ~60%
        batch_df = _build_base_batch(people, addresses, "Crocevia", start_index, total_rows)
        _write_batch(session, batch_df, CROCEVIA_TABLE, first_batch)
        first_batch = False
        generated += batch
        start_index += batch
        print(f"CROCEVIA: wrote {generated:,}/{total_rows:,}")


def generate_summit(session: snowpark.Session, total_rows: int) -> None:
    # For overlap, we need access to a pool from Crocevia table in Snowflake to avoid holding 6M locally
    # We will sample a pool of Crocevia rows per batch to serve as overlap sources.
    first_batch = True
    generated = 0
    start_index = 0
    plan = OverlapPlan().normalized()
    while generated < total_rows:
        batch = min(BATCH_SIZE_SUMMIT, total_rows - generated)
        people = load_people(session, batch)
        addresses = load_addresses(session, int(batch * 0.6))
        base_df = _build_base_batch(people, addresses, "Summit Sports", start_index, total_rows)

        # Sample a Crocevia pool from Snowflake for overlap application
        pool_size = int(batch * SUMMIT_OVERLAP_RATIO * 1.2)  # slightly larger pool for randomness
        pool_query = f"""
            SELECT FIRST_NAME, LAST_NAME, BIRTH_DATE, PHONE, EMAIL, STREET, POSTAL_CODE
            FROM {CROCEVIA_TABLE}
            ORDER BY RANDOM()
            LIMIT {pool_size}
        """
        crocevia_pool = session.sql(pool_query).to_pandas()

        overlapped_df = _apply_overlap_to_summit_batch(
            base_df, crocevia_pool, overlap_ratio=SUMMIT_OVERLAP_RATIO, plan=plan
        )
        _write_batch(session, overlapped_df, SUMMIT_TABLE, first_batch)
        first_batch = False
        generated += batch
        start_index += batch
        print(f"SUMMIT: wrote {generated:,}/{total_rows:,}")


def main(session: snowpark.Session) -> snowpark.DataFrame:
    print("Starting dual CRM generation...")
    print(f"Target Crocevia rows: {TARGET_ROWS_CROCEVIA:,}")
    print(f"Target Summit Sports rows: {TARGET_ROWS_SUMMIT:,}")

    print("Generating Crocevia CRM...")
    generate_crocevia(session, TARGET_ROWS_CROCEVIA)

    print("Generating Summit Sports CRM with overlaps...")
    generate_summit(session, TARGET_ROWS_SUMMIT)

    print("Generation complete. Returning sample from both tables...")
    sample_query = f"""
        SELECT 'CROCEVIA' AS SOURCE, * FROM {CROCEVIA_TABLE} SAMPLE ROW (100) UNION ALL
        SELECT 'SUMMIT' AS SOURCE, * FROM {SUMMIT_TABLE}  SAMPLE ROW (100)
    """
    sample_pdf = session.sql(sample_query).to_pandas()
    return session.create_dataframe(sample_pdf)


# Note: This script follows the pattern of other generators (main(session) entrypoint).
# It intentionally avoids a __main__ entrypoint to be compatible with Snowpark handler use.


