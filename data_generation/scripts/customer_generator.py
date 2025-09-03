"""
Customer data generator for Summit Sports.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from .base_generator import BaseDataGenerator


class CustomerGenerator(BaseDataGenerator):
    """
    Generates synthetic customer data for Summit Sports.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Sports-related data
        self.sports_interests = [
            'Running', 'Cycling', 'Swimming', 'Tennis', 'Basketball', 
            'Football', 'Soccer', 'Hiking', 'Skiing', 'Snowboarding',
            'Golf', 'Baseball', 'Volleyball', 'Climbing', 'Yoga'
        ]
        
        self.customer_segments = [
            'Casual Athlete', 'Serious Amateur', 'Professional', 
            'Weekend Warrior', 'Fitness Enthusiast'
        ]
    
    def generate_data(self, num_records: int) -> pd.DataFrame:
        """
        Generate customer data.
        
        Args:
            num_records: Number of customer records to generate
            
        Returns:
            DataFrame with customer data
        """
        customers = []
        
        for i in range(num_records):
            # Basic demographics
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            email = f"{first_name.lower()}.{last_name.lower()}{self.fake.random_int(1, 999)}@{self.fake.free_email_domain()}"
            
            # Registration date (within the configured date range)
            start_date = datetime.strptime(self.get_config_value('generation.start_date'), '%Y-%m-%d')
            end_date = datetime.strptime(self.get_config_value('generation.end_date'), '%Y-%m-%d')
            registration_date = self.fake.date_between(start_date=start_date, end_date=end_date)
            
            # Customer attributes
            customer = {
                'customer_id': f'CUST_{i+1:06d}',
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': self.fake.phone_number(),
                'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'gender': self.fake.random_element(['M', 'F', 'Other']),
                'registration_date': registration_date,
                'address_line1': self.fake.street_address(),
                'city': self.fake.city(),
                'state': self.fake.state_abbr(),
                'postal_code': self.fake.zipcode(),
                'country': 'USA',
                'primary_sport_interest': self.fake.random_element(self.sports_interests),
                'secondary_sport_interest': self.fake.random_element(self.sports_interests),
                'customer_segment': self.fake.random_element(self.customer_segments),
                'loyalty_points': self.fake.random_int(0, 5000),
                'is_premium_member': self.fake.boolean(chance_of_getting_true=20),
                'marketing_opt_in': self.fake.boolean(chance_of_getting_true=70),
                'last_activity_date': self.fake.date_between(start_date=registration_date, end_date=datetime.now()),
                'lifetime_value': round(self.fake.random.uniform(50, 2500), 2),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            customers.append(customer)
        
        return pd.DataFrame(customers)


def main():
    """
    Example usage of the CustomerGenerator.
    """
    generator = CustomerGenerator()
    
    # Generate data
    num_customers = generator.get_config_value('generation.customers', 1000)
    print(f"Generating {num_customers} customer records...")
    
    customer_data = generator.generate_data(num_customers)
    
    # Save data
    output_path = generator.save_data(customer_data, 'customers')
    print(f"Customer data saved to: {output_path}")
    
    # Display sample
    print("\nSample data:")
    print(customer_data.head())
    print(f"\nTotal records: {len(customer_data)}")


if __name__ == "__main__":
    main()
