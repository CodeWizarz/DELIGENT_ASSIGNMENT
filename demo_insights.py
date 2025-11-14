# demo_insights.py
"""
Quick demo to showcase key insights from your data
"""
import sqlite3
import pandas as pd

def show_demo_insights():
    conn = sqlite3.connect('database/ecommerce_analytics.sqlite')
    
    print("🚀 KEY BUSINESS INSIGHTS DEMO")
    print("=" * 50)
    
    # Top products
    top_products = pd.read_sql("""
        SELECT product_name, SUM(subtotal) as revenue 
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY product_name 
        ORDER BY revenue DESC 
        LIMIT 5
    """, conn)
    
    print("\n📈 Top 5 Products by Revenue:")
    print(top_products)
    
    # Customer segments
    segments = pd.read_sql("""
        SELECT customer_tier, COUNT(*) as count, AVG(total_amount) as avg_order_value
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY customer_tier
    """, conn)
    
    print("\n👥 Customer Segmentation:")
    print(segments)

if __name__ == "__main__":
    show_demo_insights()