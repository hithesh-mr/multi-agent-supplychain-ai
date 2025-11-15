import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

class SupplyChainDB:
    # Maps table names to their primary key columns so duplicates can be dropped before insert
    PRIMARY_KEYS = {
        "vendor": ["vendor_id"],
        "product": ["sku_id"],
        "warehouse": ["warehouse_id"],
        "purchase_order": ["po_id"],
        "shipment": ["shipment_id"],
        "inventory": ["warehouse_id", "sku_id"],
        "demand": ["date", "sku_id"],
    }
    def __init__(self, db_path: str = "data/database/supply_chain.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None

    def connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        return self.conn

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def create_tables(self):
        """Create database tables with constraints and indexes"""
        with self.connect() as conn:
            conn.executescript("""
        
        PRAGMA foreign_keys = ON;

        ------------------------------
        -- Vendor
        ------------------------------
        CREATE TABLE IF NOT EXISTS vendor (
            vendor_id TEXT PRIMARY KEY,
            supplier_name TEXT NOT NULL,
            defect_rate REAL CHECK (defect_rate BETWEEN 0 AND 1),
            lead_time_days INTEGER CHECK (lead_time_days >= 0),
            quality_score REAL CHECK (quality_score BETWEEN 0 AND 5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        ------------------------------
        -- Warehouse
        ------------------------------
        CREATE TABLE IF NOT EXISTS warehouse (
            warehouse_id TEXT PRIMARY KEY,
            location TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        ------------------------------
        -- Product
        ------------------------------
        CREATE TABLE IF NOT EXISTS product (
            sku_id TEXT PRIMARY KEY,
            product_type TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        ------------------------------
        -- Purchase Order
        ------------------------------
        CREATE TABLE IF NOT EXISTS purchase_order (
            po_id TEXT PRIMARY KEY,
            sku_id TEXT NOT NULL,
            vendor_id TEXT NOT NULL,
            po_date DATE NOT NULL,
            po_qty REAL NOT NULL CHECK (po_qty > 0),
            promised_delivery_date DATE NOT NULL,
            actual_receipt_qty REAL CHECK (actual_receipt_qty >= 0),
            inspection_results TEXT CHECK (inspection_results IN ('pass', 'fail', 'pending')),
            manufacturing_costs REAL CHECK (manufacturing_costs >= 0),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (sku_id) REFERENCES product(sku_id) ON DELETE CASCADE,
            FOREIGN KEY (vendor_id) REFERENCES vendor(vendor_id) ON DELETE CASCADE,
            CHECK (promised_delivery_date >= po_date)
        );

        ------------------------------
        -- Inventory
        ------------------------------
        CREATE TABLE IF NOT EXISTS inventory (
            warehouse_id TEXT NOT NULL,
            sku_id TEXT NOT NULL,

            stock_available INTEGER NOT NULL DEFAULT 0 CHECK (stock_available >= 0),
            on_hand_qty INTEGER NOT NULL DEFAULT 0 CHECK (on_hand_qty >= 0),
            in_transit_qty INTEGER NOT NULL DEFAULT 0 CHECK (in_transit_qty >= 0),
            reorder_point INTEGER NOT NULL DEFAULT 0 CHECK (reorder_point >= 0),
            safety_stock INTEGER NOT NULL DEFAULT 0 CHECK (safety_stock >= 0),

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (warehouse_id, sku_id),

            FOREIGN KEY (warehouse_id) REFERENCES warehouse(warehouse_id) ON DELETE CASCADE,
            FOREIGN KEY (sku_id) REFERENCES product(sku_id) ON DELETE CASCADE
        );

        ------------------------------
        -- Shipment (Linked to PO)
        ------------------------------
        CREATE TABLE IF NOT EXISTS shipment (
            shipment_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,  -- purchase_order.po_id
            origin_lat REAL CHECK (origin_lat BETWEEN -90 AND 90),
            origin_lng REAL CHECK (origin_lng BETWEEN -180 AND 180),
            destination_lat REAL CHECK (destination_lat BETWEEN -90 AND 90),
            destination_lng REAL CHECK (destination_lng BETWEEN -180 AND 180),
            status TEXT NOT NULL CHECK (status IN ('pending','in_transit','delivered','delayed','cancelled')),
            event_timestamp DATETIME NOT NULL,
            estimated_delivery_date DATE,
            actual_delivery_date DATE,
            delay_hours REAL DEFAULT 0 CHECK (delay_hours >= 0),
            shipping_carrier TEXT NOT NULL,
            shipping_time_days INTEGER CHECK (shipping_time_days >= 0),
            shipping_cost REAL CHECK (shipping_cost >= 0),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            CHECK (actual_delivery_date IS NULL OR actual_delivery_date >= event_timestamp),

            FOREIGN KEY (order_id) REFERENCES purchase_order(po_id) ON DELETE CASCADE
        );

        ------------------------------
        -- Demand
        ------------------------------
        CREATE TABLE IF NOT EXISTS demand (
            date DATE NOT NULL,
            sku_id TEXT NOT NULL,

            price REAL CHECK (price >= 0),
            discount_percent REAL CHECK (discount_percent BETWEEN 0 AND 100),
            competitor_price REAL CHECK (competitor_price >= 0),
            web_traffic INTEGER CHECK (web_traffic >= 0),
            units_sold INTEGER CHECK (units_sold >= 0),

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (date, sku_id),

            FOREIGN KEY (sku_id) REFERENCES product(sku_id) ON DELETE CASCADE
        );

        ------------------------------
        -- Indexes
        ------------------------------
        CREATE INDEX IF NOT EXISTS idx_po_sku ON purchase_order(sku_id);
        CREATE INDEX IF NOT EXISTS idx_po_vendor ON purchase_order(vendor_id);
        CREATE INDEX IF NOT EXISTS idx_po_dates ON purchase_order(po_date, promised_delivery_date);

        CREATE INDEX IF NOT EXISTS idx_inventory_warehouse ON inventory(warehouse_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_sku ON inventory(sku_id);

        CREATE INDEX IF NOT EXISTS idx_shipment_status ON shipment(status);
        CREATE INDEX IF NOT EXISTS idx_shipment_dates ON shipment(event_timestamp);

        CREATE INDEX IF NOT EXISTS idx_demand_date ON demand(date);
        CREATE INDEX IF NOT EXISTS idx_demand_sku ON demand(sku_id);

        """)


    def load_data_from_csv(self, table_name: str, csv_path: Path, date_columns=None, fk_filters=None):
        """Load data from CSV into specified table, replacing existing data"""
        if not csv_path.exists():
            print(f"Warning: {csv_path} not found. Skipping {table_name} data load.")
            return

        print(f"Loading {table_name} data from {csv_path}...")
        df = pd.read_csv(csv_path)
        
        pk_subset = self.PRIMARY_KEYS.get(table_name)
        duplicates = df.duplicated(subset=pk_subset)
        if duplicates.any():
            print(f"Warning: Found {duplicates.sum()} duplicate rows in {table_name}")
            print("Duplicate values:", df[duplicates].head())

        if pk_subset:
            before_drop = len(df)
            df = df.drop_duplicates(subset=pk_subset, keep="first")
            dropped = before_drop - len(df)
            if dropped:
                print(f"Removed {dropped} duplicate rows from {table_name} using keys {pk_subset}")

        # Convert date columns
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.date

        with self.connect() as conn:
            # Filter rows against foreign key constraints if requested
            if fk_filters:
                for column, query in fk_filters.items():
                    if column not in df.columns:
                        continue
                    allowed = {row[0] for row in conn.execute(query)}
                    before = len(df)
                    df = df[df[column].isin(allowed)]
                    dropped = before - len(df)
                    if dropped:
                        print(f"Removed {dropped} rows from {table_name} due to missing FK values in {column}")
            # First clear existing data
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_name};")
            conn.commit()
            
            # Then insert new data
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Loaded {len(df)} rows into {table_name}")
            conn.commit()

    def verify_tables(self):
        """Verify tables and row counts"""
        with self.connect() as conn:
            tables = pd.read_sql("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """, conn)
            
            print("\nTable Verification:")
            print("=" * 50)
            for _, row in tables.iterrows():
                table_name = row['name']
                count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn).iloc[0]['count']
                print(f"{table_name}: {count} rows")

def main(recreate_tables=False):
    """Initialize and populate the database.
    
    Args:
        recreate_tables (bool): If True, drops and recreates all tables.
                               If False, uses existing tables.
    """
    # Initialize database
    db = SupplyChainDB("data/database/supply_chain.db")
    
    try:
        if recreate_tables:
            print("Dropping and recreating tables...")
            # First drop all tables
            with db.connect() as conn:
                conn.execute("PRAGMA foreign_keys = OFF;")
                cursor = conn.cursor()
                
                # Get all table names
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' 
                    AND name NOT LIKE 'sqlite_%';
                """)
                tables = [row[0] for row in cursor.fetchall()]
                
                # Drop all tables
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table};")
                
                conn.commit()
                conn.execute("PRAGMA foreign_keys = ON;")
            
            # Recreate all tables
            db.create_tables()
        else:
            print("Using existing tables...")
            db.create_tables()  # This will create tables only if they don't exist
        
        # Rest of your code remains the same
        data_dir = Path("data/derived")
        
        # Load data (order matters due to foreign key constraints)
        db.load_data_from_csv("product", data_dir / "products_data.csv")
        db.load_data_from_csv("warehouse", data_dir / "warehouse_data.csv")
        db.load_data_from_csv("vendor", data_dir / "vendor_data.csv")
        db.load_data_from_csv("purchase_order", data_dir / "purchase_order_data.csv", 
                            ["po_date", "promised_delivery_date"])
        db.load_data_from_csv("inventory", data_dir / "inventory_data.csv")
        db.load_data_from_csv(
            "shipment",
            data_dir / "shipment_data.csv",
            ["event_timestamp", "estimated_delivery_date", "actual_delivery_date"],
            fk_filters={"order_id": "SELECT po_id FROM purchase_order;"},
        )
        db.load_data_from_csv("demand", data_dir / "demand_data.csv", ["date"])
        
        # Verify data
        db.verify_tables()
        print("\nDatabase setup completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize supply chain database')
    parser.add_argument('--recreate', action='store_true',
                      help='Drop and recreate all tables')
    
    args = parser.parse_args()
    main(recreate_tables=args.recreate)
