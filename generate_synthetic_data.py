# quick_fix.py
"""
Quick data generation to test the pipeline - FIXED VERSION
"""
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path

def generate_quick_data():
    """Generate minimal viable dataset for testing"""
    print("🔄 Generating quick test data...")
    
    fake = Faker()
    np.random.seed(42)
    Faker.seed(42)
    
    # Create directories
    Path('data').mkdir(exist_ok=True)
    Path('database').mkdir(exist_ok=True)
    
    # Generate customers
    customers = pd.DataFrame({
        'customer_id': range(1, 51),
        'first_name': [fake.first_name() for _ in range(50)],
        'last_name': [fake.last_name() for _ in range(50)],
        'email': [fake.email() for _ in range(50)],
        'signup_date': [fake.date_between(start_date='-365d', end_date='today') for _ in range(50)],
        'customer_tier': np.random.choice(['standard', 'premium'], 50, p=[0.7, 0.3]),
        'country': [fake.country() for _ in range(50)],
        'city': [fake.city() for _ in range(50)],
        'loyalty_score': np.random.normal(50, 20, 50),
        'is_active': np.random.choice([True, False], 50, p=[0.9, 0.1])
    })
    
    # Generate products
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books']
    products = pd.DataFrame({
        'product_id': range(1, 31),
        'product_name': [f"{fake.word().title()} {fake.word().title()}" for _ in range(30)],
        'category': np.random.choice(categories, 30),
        'subcategory': [fake.word() for _ in range(30)],
        'price': np.round(np.random.uniform(10, 200, 30), 2),
        'cost_price': np.round(np.random.uniform(5, 100, 30), 2),
        'stock_quantity': np.random.randint(10, 100, 30),
        'supplier': [fake.company() for _ in range(30)],
        'creation_date': [fake.date_between(start_date='-730d', end_date='today') for _ in range(30)],
        'is_active': np.random.choice([True, False], 30, p=[0.95, 0.05])
    })
    
    # Generate orders - FIXED DATE GENERATION
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)  # 6 months ago
    
    orders = pd.DataFrame({
        'order_id': range(1, 101),
        'customer_id': np.random.randint(1, 51, 100),
        'order_date': [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(100)],
        'status': np.random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled'], 100, p=[0.1, 0.2, 0.3, 0.35, 0.05]),
        'shipping_country': [fake.country() for _ in range(100)],
        'shipping_city': [fake.city() for _ in range(100)],
        'total_amount': np.round(np.random.uniform(20, 500, 100), 2)
    })
    
    # Generate order items
    order_items = []
    item_id = 1
    for order_id in range(1, 101):
        num_items = np.random.randint(1, 4)
        for _ in range(num_items):
            product_id = np.random.randint(1, 31)
            quantity = np.random.randint(1, 3)
            # Get the product price safely
            product_row = products[products['product_id'] == product_id]
            if len(product_row) > 0:
                unit_price = product_row['price'].values[0]
                order_items.append({
                    'order_item_id': item_id,
                    'order_id': order_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'subtotal': round(quantity * unit_price, 2)
                })
                item_id += 1
    
    order_items_df = pd.DataFrame(order_items)
    
    # Generate payments
    payments = pd.DataFrame({
        'payment_id': range(1, 101),
        'order_id': range(1, 101),
        'payment_method': np.random.choice(['credit_card', 'debit_card', 'paypal'], 100),
        'amount': orders['total_amount'],
        'payment_status': np.random.choice(['completed', 'failed', 'refunded'], 100, p=[0.9, 0.05, 0.05]),
        'payment_date': orders['order_date'],
        'transaction_id': [fake.uuid4()[:16] for _ in range(100)],
        'risk_score': np.round(np.random.normal(0.1, 0.3, 100), 3)
    })
    
    # Save to CSV
    customers.to_csv('data/customers.csv', index=False)
    products.to_csv('data/products.csv', index=False)
    orders.to_csv('data/orders.csv', index=False)
    order_items_df.to_csv('data/order_items.csv', index=False)
    payments.to_csv('data/payments.csv', index=False)
    
    print("✅ Quick test data generated successfully!")
    print(f"📁 Created: {len(customers)} customers, {len(products)} products, {len(orders)} orders")
    return customers, products, orders, order_items_df, payments

def create_quick_database():
    """Create SQLite database with the quick data"""
    print("🔄 Creating database...")
    
    conn = sqlite3.connect('database/ecommerce_analytics.sqlite')
    
    # Read and insert data
    tables = {
        'customers': pd.read_csv('data/customers.csv'),
        'products': pd.read_csv('data/products.csv'),
        'orders': pd.read_csv('data/orders.csv'),
        'order_items': pd.read_csv('data/order_items.csv'),
        'payments': pd.read_csv('data/payments.csv')
    }
    
    for table_name, df in tables.items():
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"   ✅ {table_name}: {len(df)} rows")
    
    conn.close()
    print("✅ Database created successfully!")

if __name__ == "__main__":
    generate_quick_data()
    create_quick_database()
    print("🎉 Quick setup completed! Now run: python demo_insights.py")