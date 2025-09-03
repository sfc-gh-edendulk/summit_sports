import snowflake.snowpark as snowpark
#from snowflake.snowpark.functions import col, rand, when, lit
import pandas as pd
import random
import faker
from faker.providers import person, address, internet, date_time

def generate_customers(num_customers, store_ids):
    fake = faker.Faker("fr_FR")
    customers = []
    for _ in range(num_customers):
        customer_id = fake.unique.uuid4()
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email() if random.random() > 0.15 else None
        phone = fake.unique.phone_number() if random.random() > 0.20 else None
        registration_date = fake.date_this_century()
        preferred_store = random.choice(store_ids)
        marketing_opt_in = random.choice([True, False])
        customers.append((customer_id, first_name, last_name, email, phone, registration_date, preferred_store, marketing_opt_in))
    return pd.DataFrame(customers, columns=["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", "REGISTRATION_DATE", "PREFERRED_STORE", "MARKETING_OPT_IN"])

def generate_loyalty_cards(customers_df):
    fake = faker.Faker("fr_FR")
    loyalty_cards = []
    for _, row in customers_df.iterrows():
        num_cards = random.randint(1, 3) if random.random() < 0.05 else 1
        for _ in range(num_cards):
            loyalty_cards.append((
                fake.unique.uuid4(),
                row["CUSTOMER_ID"],
                row["REGISTRATION_DATE"],
                random.choice(["ACTIVE", "LOST", "EXPIRED"]),
                random.randint(0, 50000),
                fake.date_this_decade()
            ))
    return pd.DataFrame(loyalty_cards, columns=["CARD_ID", "CUSTOMER_ID", "CARD_ISSUE_DATE", "CARD_STATUS", "POINTS_BALACE", "LAST_USE_DATE"])

def link_transactions_to_customers(session, customers_df, loyalty_cards_df):
    transactions_df = session.table("SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_INDEXED").to_pandas()
    transactions_df["CUSTOMER_ID"] = None
    transactions_df["CARD_ID"] = None
    transactions_df["PAYMENT_METHOD"] = transactions_df["PAYMENT_METHOD"].apply(lambda x: "CASH" if random.random() < 0.40 else x)
    
    for i in range(len(transactions_df)):
        if random.random() > 0.30:
            linked_customer = customers_df.sample(1).iloc[0]
            transactions_df.at[i, "CUSTOMER_ID"] = linked_customer["CUSTOMER_ID"]
            if random.random() > 0.20:
                linked_card = loyalty_cards_df[loyalty_cards_df["CUSTOMER_ID"] == linked_customer["CUSTOMER_ID"]].sample(1)["CARD_ID"].values[0]
                transactions_df.at[i, "CARD_ID"] = linked_card
    
    session.write_pandas(transactions_df, "SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_CRM", auto_create_table=True, overwrite=True)

def main(session: snowpark.Session):
    
    store_catalogue = session.table("SPORTS_DB.SPORTS_DATA.SPORTS_STORES")
    store_ids = store_catalogue.select("STOREID").to_pandas()["STOREID"].tolist()
    
    print("Generating customers...")
    customers_df = generate_customers(500_000, store_ids)
    
    print("Generating loyalty cards...")
    loyalty_cards_df = generate_loyalty_cards(customers_df)
    
    print("Writing customers to Snowflake...")
    session.write_pandas(customers_df, "CUSTOMERS", auto_create_table=True, overwrite=True)
    
    print("Writing loyalty cards to Snowflake...")
    session.write_pandas(loyalty_cards_df, "FIDELITY_CARDS", auto_create_table=True, overwrite=True)
    
    print("Linking transactions to customers...")
    link_transactions_to_customers(session, customers_df, loyalty_cards_df)
    
    print("CRM data generation complete!")
    
    return customers_df.sample(n=100)
