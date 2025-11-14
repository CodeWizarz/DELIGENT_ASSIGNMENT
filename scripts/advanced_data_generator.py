# scripts/advanced_data_generator.py
"""
Synthetic E-commerce Data Generator
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any
import json
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdvancedEcommerceDataGenerator:
    """Advanced data generator with realistic business patterns"""
    
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        self.rng = np.random.default_rng(seed)
        Faker.seed(seed)
        
        # Business configuration
        self.config = {
            'customer_count': 300,
            'product_count': 150,
            'order_count': 800,
            'date_range': (datetime(2023, 1, 1), datetime(2024, 1, 1)),
            'price_range': (5.0, 500.0),
            'categories': ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Beauty'],
            'payment_methods': ['credit_card', 'debit_card', 'paypal', 'apple_pay'],
            'order_statuses': ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        }
        
    def generate_customers(self) -> pd.DataFrame:
        """Generate customers with realistic demographics and behavior patterns"""
        logger.info("Generating advanced customer dataset...")
        
        customers = []
        for i in range(self.config['customer_count']):
            signup_date = self.fake.date_between(
                start_date=self.config['date_range'][0] - timedelta(days=365),
                end_date=self.config['date_range'][1]
            )
            
            # Customer tier based on signup date and random factor
            days_since_signup = (self.config['date_range'][1] - signup_date).days
            tier_probability = min(0.8, days_since_signup / 365)
            tier = 'premium' if self.rng.random() < tier_probability * 0.3 else 'standard'
            
            customers.append({
                'customer_id': i + 1,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'signup_date': signup_date,
                'customer_tier': tier,
                'country': self.fake.country(),
                'city': self.fake.city(),
                'loyalty_score': self.rng.normal(50, 20),  # Synthetic loyalty metric
                'is_active': self.rng.random() > 0.1  # 90% active rate
            })
        
        df = pd.DataFrame(customers)
        logger.info(f"Generated {len(df)} customers with advanced attributes")
        return df
    
    def generate_products(self) -> pd.DataFrame:
        """Generate products with realistic pricing and category distribution"""
        logger.info("Generating advanced product catalog...")
        
        products = []
        categories = self.config['categories']
        
        for i in range(self.config['product_count']):
            category = self.rng.choice(categories)
            base_price = self.rng.uniform(*self.config['price_range'])
            
            # Price variations based on category
            price_multipliers = {
                'Electronics': 1.2,
                'Beauty': 1.1,
                'Clothing': 1.0,
                'Sports': 0.9,
                'Home & Garden': 0.8,
                'Books': 0.7
            }
            
            price = round(base_price * price_multipliers.get(category, 1.0), 2)
            
            # Stock level based on category and price
            expensive = price > 100
            stock_ranges = {
                'Electronics': (5, 50) if expensive else (10, 100),
                'Clothing': (20, 200),
                'Books': (50, 500),
                'Sports': (10, 100),
                'Home & Garden': (15, 150),
                'Beauty': (25, 250)
            }
            
            stock_min, stock_max = stock_ranges.get(category, (10, 100))
            stock_quantity = self.rng.integers(stock_min, stock_max)
            
            products.append({
                'product_id': i + 1,
                'product_name': f"{self.fake.word().title()} {self.fake.word().title()}",
                'category': category,
                'subcategory': self.fake.word(),
                'price': price,
                'cost_price': round(price * self.rng.uniform(0.3, 0.7), 2),
                'stock_quantity': stock_quantity,
                'supplier': self.fake.company(),
                'creation_date': self.fake.date_between(
                    start_date=datetime(2022, 1, 1),
                    end_date=self.config['date_range'][1]
                ),
                'is_active': self.rng.random() > 0.05  # 95% active products
            })
        
        df = pd.DataFrame(products)
        logger.info(f"Generated {len(df)} products with inventory tracking")
        return df
    
    def generate_orders_and_items(self, customers: pd.DataFrame, products: pd.DataFrame) -> tuple:
        """Generate orders with realistic temporal patterns and order items"""
        logger.info("Generating orders with business patterns...")
        
        orders = []
        order_items = []
        order_id = 1
        item_id = 1
        
        active_customers = customers[customers['is_active']]
        active_products = products[products['is_active']]
        
        # Generate orders with seasonal patterns
        current_date = self.config['date_range'][0]
        while current_date <= self.config['date_range'][1]:
            # Seasonal order volume adjustment
            day_of_week = current_date.weekday()
            month = current_date.month
            
            # Business patterns: more orders on weekends, holidays, etc.
            base_orders_per_day = 2
            if day_of_week >= 5:  # Weekend
                base_orders_per_day += 1
            if month in [11, 12]:  # Holiday season
                base_orders_per_day += 1
            
            daily_orders = self.rng.poisson(base_orders_per_day)
            
            for _ in range(daily_orders):
                customer = active_customers.iloc[self.rng.integers(0, len(active_customers))]
                
                # Order status with realistic progression
                days_ago = (self.config['date_range'][1] - current_date).days
                status_weights = [0.05, 0.1, 0.3, 0.5, 0.05]  # More recent orders more likely to be in progress
                status_idx = min(4, max(0, 4 - days_ago // 7))
                status = self.config['order_statuses'][status_idx]
                
                order = {
                    'order_id': order_id,
                    'customer_id': customer['customer_id'],
                    'order_date': current_date,
                    'status': status,
                    'shipping_country': customer['country'],
                    'shipping_city': customer['city'],
                    'total_amount': 0  # Will be calculated from items
                }
                
                # Generate order items
                num_items = self.rng.integers(1, 5)
                order_total = 0
                
                for _ in range(num_items):
                    product = active_products.iloc[self.rng.integers(0, len(active_products))]
                    quantity = self.rng.integers(1, 3)
                    
                    # Price might differ from current product price (sales, discounts)
                    actual_price = product['price'] * self.rng.uniform(0.8, 1.0)  # Possible discount
                    
                    order_items.append({
                        'order_item_id': item_id,
                        'order_id': order_id,
                        'product_id': product['product_id'],
                        'quantity': quantity,
                        'unit_price': round(actual_price, 2),
                        'subtotal': round(quantity * actual_price, 2)
                    })
                    
                    order_total += quantity * actual_price
                    item_id += 1
                
                order['total_amount'] = round(order_total, 2)
                orders.append(order)
                order_id += 1
            
            current_date += timedelta(days=1)
        
        orders_df = pd.DataFrame(orders)
        items_df = pd.DataFrame(order_items)
        
        logger.info(f"Generated {len(orders_df)} orders and {len(items_df)} order items")
        return orders_df, items_df
    
    def generate_payments(self, orders: pd.DataFrame) -> pd.DataFrame:
        """Generate payment records with realistic failure rates and timing"""
        logger.info("Generating payment records...")
        
        payments = []
        payment_id = 1
        
        for _, order in orders.iterrows():
            payment_date = order['order_date'] + timedelta(
                hours=self.rng.integers(1, 24)
            )
            
            # Payment status logic based on order status
            if order['status'] == 'cancelled':
                payment_status = 'refunded' if self.rng.random() > 0.5 else 'failed'
            else:
                payment_status = 'completed' if self.rng.random() > 0.05 else 'failed'
            
            payment_method = self.rng.choice(self.config['payment_methods'])
            
            payments.append({
                'payment_id': payment_id,
                'order_id': order['order_id'],
                'payment_method': payment_method,
                'amount': order['total_amount'],
                'payment_status': payment_status,
                'payment_date': payment_date,
                'transaction_id': self.fake.uuid4()[:16],
                'risk_score': self.rng.normal(0.1, 0.3)  # Synthetic fraud risk score
            })
            
            payment_id += 1
        
        df = pd.DataFrame(payments)
        logger.info(f"Generated {len(df)} payment records")
        return df
    
    def generate_all_data(self) -> Dict[str, pd.DataFrame]:
        """Generate complete dataset with referential integrity"""
        logger.info("Starting advanced data generation...")
        
        customers = self.generate_customers()
        products = self.generate_products()
        orders, order_items = self.generate_orders_and_items(customers, products)
        payments = self.generate_payments(orders)
        
        # Data validation
        self._validate_data_integrity(customers, products, orders, order_items, payments)
        
        return {
            'customers': customers,
            'products': products,
            'orders': orders,
            'order_items': order_items,
            'payments': payments
        }
    
    def _validate_data_integrity(self, *dataframes):
        """Validate referential integrity and data quality"""
        logger.info("Validating data integrity...")
        
        customers, products, orders, order_items, payments = dataframes
        
        # Check foreign key relationships
        assert set(orders['customer_id']).issubset(set(customers['customer_id']))
        assert set(order_items['order_id']).issubset(set(orders['order_id']))
        assert set(order_items['product_id']).issubset(set(products['product_id']))
        assert set(payments['order_id']).issubset(set(orders['order_id']))
        
        # Check for negative values
        assert (order_items['quantity'] > 0).all()
        assert (order_items['unit_price'] >= 0).all()
        assert (products['price'] > 0).all()
        
        logger.info("✅ All data integrity checks passed!")

def main():
    """Main execution function"""
    try:
        # Initialize generator
        generator = AdvancedEcommerceDataGenerator(seed=42)
        
        # Generate all datasets
        datasets = generator.generate_all_data()
        
        # Save to CSV files
        output_dir = Path('data')
        output_dir.mkdir(exist_ok=True)
        
        for name, df in datasets.items():
            filename = output_dir / f"{name}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved {filename} with {len(df)} rows")
        
        # Generate schema documentation
        generate_schema_documentation(datasets)
        
        logger.info("🎉 Advanced data generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Data generation failed: {str(e)}")
        raise

def generate_schema_documentation(datasets: Dict[str, pd.DataFrame]):
    """Generate comprehensive schema documentation"""
    schema_info = {}
    
    for name, df in datasets.items():
        schema_info[name] = {
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'row_count': len(df),
            'description': f"Advanced {name.replace('_', ' ')} dataset with business logic"
        }
    
    with open('data/schema_documentation.json', 'w') as f:
        json.dump(schema_info, f, indent=2)
    
    logger.info("📚 Schema documentation generated")

if __name__ == "__main__":
    main()