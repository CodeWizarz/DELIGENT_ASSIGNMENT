# scripts/advanced_ingestion.py
"""
Advanced Data Ingestion Engine with SQLAlchemy ORM
Features:
- Schema validation and migration
- Comprehensive error handling
- Data quality checks
- Performance optimization
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import sys

# Configure advanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class AdvancedCustomer(Base):
    __tablename__ = 'customers'
    
    customer_id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    signup_date = Column(DateTime, nullable=False)
    customer_tier = Column(String(20), nullable=False)
    country = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    loyalty_score = Column(Float, nullable=False)
    is_active = Column(Boolean, default=True)

class AdvancedProduct(Base):
    __tablename__ = 'products'
    
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(255), nullable=False)
    category = Column(String(100), nullable=False)
    subcategory = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    cost_price = Column(Float, nullable=False)
    stock_quantity = Column(Integer, nullable=False)
    supplier = Column(String(255), nullable=False)
    creation_date = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)

class AdvancedOrder(Base):
    __tablename__ = 'orders'
    
    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, sa.ForeignKey('customers.customer_id'), nullable=False)
    order_date = Column(DateTime, nullable=False)
    status = Column(String(50), nullable=False)
    shipping_country = Column(String(100), nullable=False)
    shipping_city = Column(String(100), nullable=False)
    total_amount = Column(Float, nullable=False)

class AdvancedOrderItem(Base):
    __tablename__ = 'order_items'
    
    order_item_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, sa.ForeignKey('orders.order_id'), nullable=False)
    product_id = Column(Integer, sa.ForeignKey('products.product_id'), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    subtotal = Column(Float, nullable=False)

class AdvancedPayment(Base):
    __tablename__ = 'payments'
    
    payment_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, sa.ForeignKey('orders.order_id'), nullable=False)
    payment_method = Column(String(50), nullable=False)
    amount = Column(Float, nullable=False)
    payment_status = Column(String(50), nullable=False)
    payment_date = Column(DateTime, nullable=False)
    transaction_id = Column(String(50), unique=True, nullable=False)
    risk_score = Column(Float, nullable=False)

class AdvancedDataIngestor:
    """Advanced data ingestion engine with comprehensive features"""
    
    def __init__(self, db_path: str = 'database/ecommerce_analytics.sqlite'):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = None
        self.Session = None
        
    def initialize_database(self) -> bool:
        """Initialize database with schema and indexes"""
        try:
            database_url = f"sqlite:///{self.db_path}"
            self.engine = create_engine(database_url, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            
            # Create performance indexes
            self._create_indexes()
            
            logger.info("✅ Database initialized successfully with optimized schema")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Database initialization failed: {str(e)}")
            return False
    
    def _create_indexes(self):
        """Create performance-optimizing indexes"""
        with self.engine.connect() as conn:
            # Customer indexes
            conn.execute(sa.text("""
                CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
                CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);
                CREATE INDEX IF NOT EXISTS idx_customers_tier ON customers(customer_tier);
            """))
            
            # Order indexes
            conn.execute(sa.text("""
                CREATE INDEX IF NOT EXISTS idx_orders_customer_date ON orders(customer_id, order_date);
                CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
                CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
            """))
            
            # Order items indexes
            conn.execute(sa.text("""
                CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
                CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
            """))
            
            # Payment indexes
            conn.execute(sa.text("""
                CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
                CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(payment_status);
                CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);
            """))
            
        logger.info("✅ Performance indexes created")
    
    def validate_data_quality(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Comprehensive data quality validation"""
        validation_results = {
            'is_valid': True,
            'issues': [],
            'summary': {}
        }
        
        try:
            # Check for null values in critical columns
            critical_columns = {
                'customers': ['customer_id', 'email', 'first_name', 'last_name'],
                'products': ['product_id', 'product_name', 'price'],
                'orders': ['order_id', 'customer_id', 'order_date', 'total_amount'],
                'order_items': ['order_item_id', 'order_id', 'product_id', 'quantity'],
                'payments': ['payment_id', 'order_id', 'amount', 'payment_status']
            }
            
            if table_name in critical_columns:
                for col in critical_columns[table_name]:
                    if col in df.columns and df[col].isnull().any():
                        validation_results['issues'].append(f"Null values found in critical column: {col}")
                        validation_results['is_valid'] = False
            
            # Check for negative values where inappropriate
            if 'quantity' in df.columns and (df['quantity'] <= 0).any():
                validation_results['issues'].append("Invalid quantities found (<= 0)")
                validation_results['is_valid'] = False
            
            if 'price' in df.columns and (df['price'] < 0).any():
                validation_results['issues'].append("Negative prices found")
                validation_results['is_valid'] = False
            
            # Data type validation
            validation_results['summary'] = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage': df.memory_usage(deep=True).sum() / 1024**2,  # MB
                'null_count': df.isnull().sum().to_dict()
            }
            
            if validation_results['is_valid']:
                logger.info(f"✅ Data quality validation passed for {table_name}")
            else:
                logger.warning(f"⚠️ Data quality issues found for {table_name}: {validation_results['issues']}")
            
        except Exception as e:
            logger.error(f"❌ Data quality validation failed: {str(e)}")
            validation_results['is_valid'] = False
            validation_results['issues'].append(f"Validation error: {str(e)}")
        
        return validation_results
    
    def ingest_table(self, table_name: str, csv_path: Path) -> bool:
        """Ingest a single table with comprehensive error handling"""
        try:
            if not csv_path.exists():
                logger.error(f"❌ CSV file not found: {csv_path}")
                return False
            
            # Read CSV with optimized settings
            df = pd.read_csv(
                csv_path,
                parse_dates=True,
                infer_datetime_format=True,
                low_memory=False
            )
            
            # Validate data quality
            validation = self.validate_data_quality(df, table_name)
            if not validation['is_valid']:
                logger.error(f"❌ Data quality validation failed for {table_name}")
                return False
            
            # Map DataFrame to SQLAlchemy model
            model_mapping = {
                'customers': AdvancedCustomer,
                'products': AdvancedProduct,
                'orders': AdvancedOrder,
                'order_items': AdvancedOrderItem,
                'payments': AdvancedPayment
            }
            
            if table_name not in model_mapping:
                logger.error(f"❌ Unknown table: {table_name}")
                return False
            
            Model = model_mapping[table_name]
            
            # Convert DataFrame to list of model instances
            records = []
            for _, row in df.iterrows():
                record_data = {}
                for column in df.columns:
                    if hasattr(Model, column):
                        value = row[column]
                        # Handle NaN values
                        if pd.isna(value):
                            value = None
                        record_data[column] = value
                
                records.append(Model(**record_data))
            
            # Bulk insert with transaction
            session = self.Session()
            try:
                session.bulk_save_objects(records)
                session.commit()
                logger.info(f"✅ Successfully ingested {len(records)} records into {table_name}")
                return True
                
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"❌ Database error during ingestion of {table_name}: {str(e)}")
                return False
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"❌ Unexpected error during ingestion of {table_name}: {str(e)}")
            return False
    
    def ingest_all_data(self, data_dir: Path = Path('data')) -> Dict[str, bool]:
        """Ingest all CSV files with comprehensive reporting"""
        results = {}
        
        try:
            if not data_dir.exists():
                logger.error(f"❌ Data directory not found: {data_dir}")
                return results
            
            # Define ingestion order to maintain referential integrity
            ingestion_order = [
                'customers.csv',
                'products.csv', 
                'orders.csv',
                'order_items.csv',
                'payments.csv'
            ]
            
            for csv_file in ingestion_order:
                table_name = csv_file.replace('.csv', '')
                csv_path = data_dir / csv_file
                
                logger.info(f"🔄 Ingesting {table_name}...")
                success = self.ingest_table(table_name, csv_path)
                results[table_name] = success
            
            # Generate ingestion report
            self._generate_ingestion_report(results)
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Complete ingestion process failed: {str(e)}")
            return results
    
    def _generate_ingestion_report(self, results: Dict[str, bool]):
        """Generate comprehensive ingestion report"""
        report = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'database_path': str(self.db_path),
            'ingestion_results': results,
            'success_rate': f"{sum(results.values()) / len(results) * 100:.1f}%",
            'summary': f"Successfully ingested {sum(results.values())} out of {len(results)} tables"
        }
        
        # Save report
        report_path = Path('reports/ingestion_report.json')
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            import json
            json.dump(report, f, indent=2)
        
        logger.info(f"📊 Ingestion report saved: {report_path}")

def main():
    """Main execution function for data ingestion"""
    try:
        # Initialize ingestor
        ingestor = AdvancedDataIngestor()
        
        # Initialize database
        if not ingestor.initialize_database():
            sys.exit(1)
        
        # Ingest all data
        results = ingestor.ingest_all_data()
        
        # Check results
        if all(results.values()):
            logger.info("🎉 All data ingested successfully!")
        else:
            failed_tables = [table for table, success in results.items() if not success]
            logger.error(f"❌ Failed to ingest: {', '.join(failed_tables)}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Ingestion process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()