"""
Crocevia CRM Generator - Semi-Matched CRM Creation

Creates a synthetic Crocevia grocery chain CRM with controlled overlaps
to Summit Sports CRM for customer identity resolution testing.

Based on the simplified pattern from sportsretailer_crm_generator.py:
- Direct function-based approach
- Clear data generation steps
- Focused on semi-matched CRM creation
- French localization with Faker

Usage:
    python crocevia_crm_generator.py
"""

import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
import random
from faker import Faker


def load_french_addresses(session: snowpark.Session, num_addresses: int, unique_pct: float = 0.80) -> list:
    """
    Load real French addresses from CROCEVIA_DB.RAW_DATA.ADRESSES_FRANCE table.
    
    Args:
        session: Snowflake Snowpark session
        num_addresses: Number of addresses to load
        unique_pct: Percentage of addresses that should be unique
    
    Returns:
        List of address dictionaries with street, city, postal_code, lat, lng
    """
    # Calculate how many unique addresses we need
    unique_count = int(num_addresses * unique_pct)
    
    # Load random sample of addresses from the French addresses table
    addresses_query = f"""
    SELECT 
        CASE 
            WHEN NUMERO IS NOT NULL AND NOM_VOIE IS NOT NULL 
            THEN CONCAT(NUMERO, ' ', NOM_VOIE)
            ELSE NOM_VOIE
        END AS street,
        NOM_COMMUNE AS city,
        CODE_POSTAL AS postal_code,
        LAT AS latitude,
        LON AS longitude
    FROM CROCEVIA_DB.RAW_DATA.ADRESSES_FRANCE
    WHERE LAT IS NOT NULL 
      AND LON IS NOT NULL 
      AND NOM_COMMUNE IS NOT NULL
      AND CODE_POSTAL IS NOT NULL
    ORDER BY RANDOM()
    LIMIT {unique_count}
    """
    
    print(f"Loading {unique_count} unique addresses from French address database...")
    addresses_df = session.sql(addresses_query).to_pandas()
    
    if addresses_df.empty:
        raise ValueError("No addresses found in CROCEVIA_DB.RAW_DATA.ADRESSES_FRANCE table")
    
    # Convert to list of dictionaries
    addresses = []
    for _, row in addresses_df.iterrows():
        addresses.append({
            "street": row["STREET"],
            "city": row["CITY"], 
            "postal_code": str(int(row["POSTAL_CODE"])) if pd.notna(row["POSTAL_CODE"]) else None,
            "latitude": float(row["LATITUDE"]) if pd.notna(row["LATITUDE"]) else None,
            "longitude": float(row["LONGITUDE"]) if pd.notna(row["LONGITUDE"]) else None
        })
    
    # Add duplicate addresses by repeating some of the unique ones for realism
    duplicate_count = num_addresses - unique_count
    if duplicate_count > 0:
        duplicate_sources = random.choices(addresses, k=duplicate_count)
        addresses.extend(duplicate_sources)
    
    # Shuffle to distribute duplicates randomly
    random.shuffle(addresses)
    
    print(f"Loaded {len(addresses)} addresses ({unique_count} unique, {duplicate_count} duplicates)")
    return addresses


def generate_crocevia_customers(source_customers_df: pd.DataFrame, 
                              session: snowpark.Session,
                              num_customers: int = 10000,
                              source_sample_size: int = 5000) -> pd.DataFrame:
    """
    Generate Crocevia customers with controlled overlaps to source Summit Sports CRM.
    
    Args:
        source_customers_df: Source Summit Sports customer data
        num_customers: Number of Crocevia customers to generate
        source_sample_size: Sample size from source for overlaps
    
    Returns:
        DataFrame with Crocevia customers including overlap metadata
    """
    fake = Faker("fr_FR")
    random.seed(42)
    fake.seed_instance(42)
    
    # Sample source data for overlaps
    source_sample = source_customers_df.sample(n=min(source_sample_size, len(source_customers_df)), random_state=42)
    
    # Extract source data for overlaps
    src_emails = source_sample['EMAIL'].dropna().astype(str).values
    src_phones = source_sample['PHONE'].dropna().astype(str).values
    src_first_names = source_sample['FIRST_NAME'].dropna().astype(str).values
    src_last_names = source_sample['LAST_NAME'].dropna().astype(str).values
    
    # French email domains for localization
    french_domains = ['gmail.com', 'orange.fr', 'free.fr', 'wanadoo.fr', 'sfr.fr', 'laposte.net']
    
    # Overlap configuration
    triple_match_count = int(num_customers * 0.20)  # 20% triple matches
    email_overlap_count = int(num_customers * 0.60)  # 60% email overlaps total
    phone_overlap_count = int(num_customers * 0.50)  # 50% phone overlaps total
    name_overlap_count = int(num_customers * 0.35)   # 35% name overlaps total
    
    # Create overlap index sets
    np.random.seed(42)
    all_indices = set(range(num_customers))
    
    # Triple matches (exact overlap on all three fields)
    triple_indices = set(np.random.choice(list(all_indices), size=triple_match_count, replace=False))
    remaining = all_indices - triple_indices
    
    # Additional overlaps (can intersect with each other but not with triple)
    email_extra = email_overlap_count - triple_match_count
    phone_extra = phone_overlap_count - triple_match_count
    name_extra = name_overlap_count - triple_match_count
    
    email_indices = set(np.random.choice(list(remaining), size=email_extra, replace=False))
    phone_indices = set(np.random.choice(list(remaining), size=phone_extra, replace=False))
    name_indices = set(np.random.choice(list(remaining), size=name_extra, replace=False))
    
    # Load addresses for 50% of customers from real French address data
    address_count = int(num_customers * 0.50)
    addresses = load_french_addresses(session, address_count, unique_pct=0.80)
    address_indices = set(np.random.choice(range(num_customers), size=address_count, replace=False))
    address_idx = 0
    
    # Generate customers
    customers = []
    
    for i in range(num_customers):
        # Determine overlap type for this customer
        overlap_type = 'NONE'
        if i in triple_indices:
            overlap_type = 'TRIPLE'
        elif i in email_indices and i in phone_indices:
            overlap_type = 'EMAIL_PHONE'
        elif i in email_indices:
            overlap_type = 'EMAIL'
        elif i in phone_indices:
            overlap_type = 'PHONE'
        elif i in name_indices:
            overlap_type = 'NAME'
        
        # Generate base data
        if overlap_type == 'TRIPLE':
            # Use same source record for consistency
            src_row = source_sample.iloc[i % len(source_sample)]
            first_name = src_row['FIRST_NAME']
            last_name = src_row['LAST_NAME']
            email = src_row['EMAIL']
            phone = src_row['PHONE']
        else:
            # Generate synthetic French names
            first_name = fake.first_name()
            last_name = fake.last_name()
            
            # Handle email overlaps
            if overlap_type in ['EMAIL', 'EMAIL_PHONE']:
                email = random.choice(src_emails)
            else:
                # Generate French email
                if random.random() < 0.7:  # 70% name-based emails
                    local = f"{first_name.lower()}.{last_name.lower()}".replace("'", "").replace("ç", "c").replace("é", "e")
                    email = f"{local}@{random.choice(french_domains)}"
                else:
                    email = fake.email()
                    local_part = email.split('@')[0]
                    email = f"{local_part}@{random.choice(french_domains)}"
            
            # Handle phone overlaps
            if overlap_type in ['PHONE', 'EMAIL_PHONE']:
                phone = random.choice(src_phones)
            else:
                phone = fake.phone_number()
            
            # Handle name overlaps
            if overlap_type == 'NAME':
                name_pair_idx = random.randint(0, len(source_sample) - 1)
                src_row = source_sample.iloc[name_pair_idx]
                first_name = src_row['FIRST_NAME']
                last_name = src_row['LAST_NAME']
        
        # Add data quality issues
        # Missing emails (15% outside email overlap sets)
        if overlap_type not in ['TRIPLE', 'EMAIL', 'EMAIL_PHONE'] and random.random() < 0.15:
            email = None
            
        # Missing phones (20% outside phone overlap sets)
        if overlap_type not in ['TRIPLE', 'PHONE', 'EMAIL_PHONE'] and random.random() < 0.20:
            phone = None
        
        # Generate other fields
        customer_id = f"CRV-{i:010d}"
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90) if random.random() < 0.40 else None
        registration_date = fake.date_this_decade()
        marketing_opt_in = random.choice([True, False])
        
        # Add address information if this customer should have one
        if i in address_indices:
            address = addresses[address_idx]
            address_idx += 1
            street = address["street"]
            city = address["city"]
            postal_code = address["postal_code"]
            latitude = address["latitude"]
            longitude = address["longitude"]
        else:
            street = None
            city = None
            postal_code = fake.postcode() if random.random() < 0.60 else None  # Keep some postal codes for non-address customers
            latitude = None
            longitude = None
        
        customers.append({
            'CUSTOMER_ID': customer_id,
            'FIRST_NAME': first_name,
            'LAST_NAME': last_name,
            'EMAIL': email,
            'PHONE': phone,
            'STREET': street,
            'CITY': city,
            'POSTAL_CODE': postal_code,
            'LATITUDE': latitude,
            'LONGITUDE': longitude,
            'DATE_OF_BIRTH': date_of_birth,
            'REGISTRATION_DATE': registration_date,
            'MARKETING_OPT_IN': marketing_opt_in,
            'OVERLAP_TYPE': overlap_type
        })
    
    return pd.DataFrame(customers)


def add_duplicate_customers(customers_df: pd.DataFrame, duplicate_pct: float = 0.10) -> pd.DataFrame:
    """
    Add duplicate customers with slight variations to simulate real-world data quality issues.
    
    Args:
        customers_df: Original customers DataFrame
        duplicate_pct: Percentage of customers to duplicate
    
    Returns:
        DataFrame with original customers plus duplicates
    """
    duplicate_count = int(len(customers_df) * duplicate_pct)
    if duplicate_count == 0:
        return customers_df
    
    # Select random customers to duplicate
    source_indices = np.random.choice(len(customers_df), size=duplicate_count, replace=False)
    duplicates = []
    
    for src_idx in source_indices:
        src_row = customers_df.iloc[src_idx].copy()
        
        # Create new customer ID with DUP suffix
        src_row['CUSTOMER_ID'] = f"{src_row['CUSTOMER_ID']}_DUP"
        src_row['OVERLAP_TYPE'] = 'DUPLICATE'
        
        # Add slight variations for realism
        if random.random() < 0.5 and src_row['EMAIL']:
            # Modify email slightly
            email_parts = str(src_row['EMAIL']).split('@')
            if len(email_parts) == 2:
                src_row['EMAIL'] = f"{email_parts[0]}{random.randint(0, 9)}@{email_parts[1]}"
        
        if random.random() < 0.5 and src_row['PHONE']:
            # Modify last digit of phone
            phone_str = str(src_row['PHONE'])
            if phone_str:
                src_row['PHONE'] = phone_str[:-1] + str(random.randint(0, 9))
        
        duplicates.append(src_row)
    
    # Combine original and duplicates
    duplicates_df = pd.DataFrame(duplicates)
    return pd.concat([customers_df, duplicates_df], ignore_index=True)


def validate_overlap_results(customers_df: pd.DataFrame, source_customers_df: pd.DataFrame) -> dict:
    """
    Validate that overlap percentages meet targets.
    
    Args:
        customers_df: Generated Crocevia customers
        source_customers_df: Original Summit Sports customers
    
    Returns:
        Dictionary with validation metrics
    """
    total_records = len(customers_df)
    overlap_counts = customers_df['OVERLAP_TYPE'].value_counts()
    
    # Calculate overlap percentages
    triple_pct = overlap_counts.get('TRIPLE', 0) / total_records
    email_pct = (overlap_counts.get('TRIPLE', 0) + overlap_counts.get('EMAIL', 0) + 
                overlap_counts.get('EMAIL_PHONE', 0)) / total_records
    phone_pct = (overlap_counts.get('TRIPLE', 0) + overlap_counts.get('PHONE', 0) + 
                overlap_counts.get('EMAIL_PHONE', 0)) / total_records
    name_pct = (overlap_counts.get('TRIPLE', 0) + overlap_counts.get('NAME', 0)) / total_records
    duplicate_pct = overlap_counts.get('DUPLICATE', 0) / total_records
    
    # Check actual overlaps with source data
    src_emails = set(source_customers_df['EMAIL'].dropna().astype(str))
    src_phones = set(source_customers_df['PHONE'].dropna().astype(str))
    src_names = set(source_customers_df['FIRST_NAME'].astype(str) + '|' + source_customers_df['LAST_NAME'].astype(str))
    
    actual_email_overlap = customers_df['EMAIL'].dropna().astype(str).isin(src_emails).sum() / total_records
    actual_phone_overlap = customers_df['PHONE'].dropna().astype(str).isin(src_phones).sum() / total_records
    actual_name_overlap = (customers_df['FIRST_NAME'].astype(str) + '|' + customers_df['LAST_NAME'].astype(str)).isin(src_names).sum() / total_records
    
    return {
        'total_records': total_records,
        'overlap_type_breakdown': overlap_counts.to_dict(),
        'target_triple_pct': triple_pct,
        'target_email_pct': email_pct,
        'target_phone_pct': phone_pct,
        'target_name_pct': name_pct,
        'duplicate_pct': duplicate_pct,
        'actual_email_overlap': actual_email_overlap,
        'actual_phone_overlap': actual_phone_overlap,
        'actual_name_overlap': actual_name_overlap
    }


def main(session: snowpark.Session) -> snowpark.DataFrame:
    """
    Main function to generate Crocevia CRM with controlled overlaps.
    
    Args:
        session: Snowflake Snowpark session
    
    Returns:
        Snowpark DataFrame with sample of generated customers
    """
    # Configuration - hardcoded for consistency with stored procedure pattern
    target_size = 10000
    source_sample_size = 5000
    print("Loading Summit Sports customer data...")
    source_customers_df = session.table("SS_101.RAW_CUSTOMER.CUSTOMER_LOYALTY").to_pandas()
    print(f"Loaded {len(source_customers_df)} source customers")
    
    print(f"Generating {target_size} Crocevia customers with controlled overlaps...")
    customers_df = generate_crocevia_customers(
        source_customers_df,
        session,
        num_customers=target_size,
        source_sample_size=source_sample_size
    )
    
    print("Adding duplicate customers for realism...")
    customers_df = add_duplicate_customers(customers_df)
    
    print("Validating overlap results...")
    validation = validate_overlap_results(customers_df, source_customers_df)
    
    print("\n=== Generation Results ===")
    print(f"Total records: {validation['total_records']:,}")
    print(f"Overlap breakdown: {validation['overlap_type_breakdown']}")
    print(f"Triple match: {validation['target_triple_pct']:.1%}")
    print(f"Email overlap: {validation['target_email_pct']:.1%} (actual: {validation['actual_email_overlap']:.1%})")
    print(f"Phone overlap: {validation['target_phone_pct']:.1%} (actual: {validation['actual_phone_overlap']:.1%})")
    print(f"Name overlap: {validation['target_name_pct']:.1%} (actual: {validation['actual_name_overlap']:.1%})")
    print(f"Duplicates: {validation['duplicate_pct']:.1%}")
    
    print("Writing Crocevia customers to Snowflake...")
    session.write_pandas(
        customers_df, 
        "CROCEVIA_CRM", 
        auto_create_table=True, 
        overwrite=True
    )
    
    print("Crocevia CRM generation complete!")
    
    # Return a Snowpark DataFrame (required for handler)
    sample_df = customers_df.sample(n=min(100, len(customers_df)))
    return session.create_dataframe(sample_df)


