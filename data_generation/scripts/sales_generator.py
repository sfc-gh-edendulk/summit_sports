from snowflake.snowpark.functions import col
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import holidays
import snowflake.snowpark as snowpark

class B2CSalesDataGenerator:
    def __init__(self, product_catalogue, store_catalogue, chdry_data):
        self.product_catalogue = product_catalogue
        self.store_catalogue = store_catalogue
        self.chdry_data = chdry_data
        self.chdry_data["DATE"] = pd.to_datetime(self.chdry_data["DATE"])
        self.avg_daily_sales_target = 10000  # Base daily sales target per store in Euros
        self.holidays = holidays.France()
        self.sales_periods = {
            "winter": [datetime(datetime.now().year, 12, 1), datetime(datetime.now().year, 2, 28)],
            "summer": [datetime(datetime.now().year, 6, 15), datetime(datetime.now().year, 8, 31)],
            "back_to_school": [datetime(datetime.now().year, 9, 1), datetime(datetime.now().year, 9, 30)],
        }
        self.payment_methods = ["Credit Card", "Debit Card", "Gift Card", "Cash"]

        # Normalize CAC 40 values
        self.chdry_data["NORMALIZED_chdry"] = (
            (self.chdry_data["CLOSE"] - self.chdry_data["CLOSE"].min()) /
            (self.chdry_data["CLOSE"].max() - self.chdry_data["CLOSE"].min())
        )

    def generate_sales_data(self, end_date=datetime(2023, 3, 14), num_days=365):
        sales_data = []
        product_catalogue_pd = self.product_catalogue.to_pandas()
        store_catalogue_pd = self.store_catalogue.to_pandas()
        
        for day_delta in range(num_days):
            date = end_date - timedelta(days=day_delta)
            if date in self.holidays:
                continue

            # Get CAC 40 normalized value for the date
            normalized_chdry = self._get_chdry_value(date)
            if normalized_chdry is None:
                continue  # Skip if no CAC 40 data

            for store_index, store in store_catalogue_pd.iterrows():
                store_sales_data = []
                # Adjust daily sales based on CAC 40 index
                store_target = self.avg_daily_sales_target * (0.7 + 0.6 * normalized_chdry)  # Scales between 70% and 130%
                peak_sales_multiplier = self._get_peak_sales_multiplier(date, store)
                store_target *= peak_sales_multiplier
                accumulated_sales = 0

                while accumulated_sales < store_target:
                    order_id = f"ORDER-{random.randint(100000000, 999999999)}"
                    num_items = random.randint(2, 5)  # Orders now frequently contain multiple products
                    order_total = 0

                    for _ in range(num_items):
                        primary_product = product_catalogue_pd.sample(1).iloc[0]
                        mrp_price = primary_product["MRP"]  # Full price
                        sales_price = primary_product["SALE_PRICE"]  # Discounted price
                        discount_amount = mrp_price - sales_price  # Calculate discount
                        quantity = 1 if random.random() < 0.8 else random.randint(2, 5)  # Most often quantity is 1

                        # Determine whether to use MRP or Sales Price based on sales periods
                        applicable_price = sales_price if any(start <= date <= end for start, end in self.sales_periods.values()) else mrp_price
                        
                        sales_entry = self._generate_sales_entry(
                            store, date, primary_product, applicable_price, discount_amount, quantity, order_id
                        )
                        store_sales_data.append(sales_entry)
                        order_total += applicable_price * quantity

                    accumulated_sales += order_total

                final_sales_data = self._normalize_sales_to_target(store_sales_data, store_target)
                sales_data.extend(final_sales_data)

        sales_df = pd.DataFrame(sales_data, columns=[
            "ORDER_ID", "STOREID", "STORE_NAME", "SALE_DATE", "PRODUCT_ID", "QUANTITY", 
            "SALES_PRICE_EURO", "DISCOUNT_AMOUNT_EURO", "PAYMENT_METHOD", "SALES_ASSISTANT_ID", "CUSTOMER_ID"
        ])
        sales_df["SALE_DATE"] = pd.to_datetime(sales_df["SALE_DATE"]).dt.date  # Ensure SALE_DATE is a date format
        return sales_df

    def _normalize_sales_to_target(self, store_sales_data, target_sales):
        actual_sales_total = sum(entry[6] for entry in store_sales_data)
        adjustment_ratio = target_sales / actual_sales_total if actual_sales_total > 0 else 1
        for entry in store_sales_data:
            entry[5] = max(1, int(entry[5] * adjustment_ratio))  # Adjust quantity
        return store_sales_data

    def _get_peak_sales_multiplier(self, date, store):
        district = store["STORE_TYPE"].lower()
        if "alpine" in district:
            return 1.5 if date.month in [12, 1, 2] else 0.8
        elif any(start <= date <= end for start, end in self.sales_periods.values()):
            return 1.3
        return 1.0

    def _get_chdry_value(self, date):
        """Retrieve the normalized CAC 40 index value for a specific date."""
        try:
            date = pd.to_datetime(date).date()  # Ensure input date is datetime.date
            self.chdry_data["DATE"] = pd.to_datetime(self.chdry_data["DATE"]).dt.date  # Convert table dates to datetime.date
    
            matching_rows = self.chdry_data[self.chdry_data["DATE"] == date]
    
            if matching_rows.empty:
                print(f"⚠️ No chdry data found for {date}")
                return None
    
            return matching_rows["NORMALIZED_chdry"].values[0]
    
        except Exception as e:
            print(f"Error fetching chdry value for {date}: {e}")
            return None

    def _generate_sales_entry(self, store, date, product, sales_price, discount_amount, quantity, order_id):
        payment_method = random.choice(self.payment_methods)
        sales_assistant_id = f"ASSISTANT_{random.randint(1, 800)}"
        customer_id = f"CUST{random.randint(100000000, 999999999)}" if random.random() > 0.2 else None
        return [
            order_id,
            store["STOREID"], 
            store["STORE_NAME"], 
            date.strftime("%Y-%m-%d"),
            product["PRODUCTID"], 
            quantity, 
            round(sales_price * quantity, 2), 
            round(discount_amount * quantity, 2),
            payment_method, 
            sales_assistant_id, 
            customer_id
        ]

def main(session: snowpark.Session): 
    product_catalogue = session.table("SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE")
    store_catalogue = session.table("SPORTS_DB.SPORTS_DATA.SPORTS_STORES")
    chdry_data = session.table("SPORTS_DB.SPORTS_DATA.chdry0120_0325_COMPLETE").to_pandas()

    sales_generator = B2CSalesDataGenerator(product_catalogue, store_catalogue, chdry_data)
    sales_df = sales_generator.generate_sales_data()
    sales_df["SALE_DATE"] = pd.to_datetime(sales_df["SALE_DATE"]).dt.date  # Ensure SALE_DATE is a date format
    sales_dfsnowpark_df = session.create_dataframe(sales_df)
    sales_dfsnowpark_df.write.mode("append").save_as_table("instore_sales_data_indexed")

    return sales_dfsnowpark_df.sample(n=100)
