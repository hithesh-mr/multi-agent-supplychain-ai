import pandas as pd
from pathlib import Path
from datetime import datetime

def prepare_products_data():
    """Prepare products data"""
    df = pd.read_csv("data/processed/supply_chain_augmented.csv")
    products = df[['sku_id', 'product_type']].drop_duplicates()
    products.to_csv("data/derived/products_data.csv", index=False)

def prepare_warehouse_data():
    """Prepare warehouse data"""
    df = pd.read_csv("data/processed/supply_chain_augmented.csv")
    warehouses = df[['warehouse_id', 'location']].drop_duplicates()
    warehouses.to_csv("data/derived/warehouse_data.csv", index=False)

def prepare_vendor_data():
    """Prepare vendor data"""
    df = pd.read_csv("data/processed/supply_chain_augmented.csv")
    vendors = df[['vendor_id', 'supplier_name', 'defect_rate', 'lead_time_days']].drop_duplicates()
    vendors['quality_score'] = 4.5  # Default quality score
    vendors.to_csv("data/derived/vendor_data.csv", index=False)

def prepare_purchase_order_data():
    """Prepare purchase order data"""
    # Load POS data
    pos = pd.read_csv("data/processed/supply_chain_pos.csv")
    
    # Augment with additional data if needed
    aug = pd.read_csv("data/processed/supply_chain_augmented.csv")
    aug_po = aug[['po_id', 'inspection_results', 'manufacturing_costs']].drop_duplicates()
    
    # Merge and clean up
    pos = pos.merge(aug_po, on='po_id', how='left')
    pos['actual_receipt_qty'] = pos['po_qty']  # Assuming all POs were received in full
    
    # Save
    pos.to_csv("data/derived/purchase_order_data.csv", index=False)

def prepare_inventory_data():
    """Prepare inventory data"""
    # This is a simplified example - adjust based on your actual inventory data
    df = pd.read_csv("data/processed/supply_chain_augmented.csv")
    inventory = df[['warehouse_id', 'sku_id', 'stock_available', 'on_hand_qty', 
                   'in_transit_qty', 'reorder_point', 'safety_stock']].drop_duplicates()
    inventory.to_csv("data/derived/inventory_data.csv", index=False)

def prepare_shipment_data():
    """Prepare shipment data"""
    df = pd.read_csv("data/processed/supply_chain_augmented.csv")
    
    # Filter out rows where order_id is null
    shipments = df[[
        'shipment_id', 'order_id', 'origin_lat', 'origin_lng', 
        'destination_lat', 'destination_lng', 'status', 'event_timestamp',
        'estimated_delivery_date', 'actual_delivery_date', 'delay_hours',
        'shipping_carrier', 'shipping_time_days', 'shipping_cost'
    ]].dropna(subset=['order_id'])  # This ensures order_id is not null
    
    # Ensure status has a default value if null
    if 'status' in shipments.columns:
        shipments['status'] = shipments['status'].fillna('pending')
    
    shipments.to_csv("data/derived/shipment_data.csv", index=False)

def prepare_demand_data():
    """Prepare demand data"""
    df = pd.read_csv("data/processed/supply_chain_augmented.csv")
    demand = df[[
        'date', 'sku_id', 'price', 'discount_percent', 'competitor_price',
        'web_traffic', 'units_sold'
    ]].drop_duplicates()
    demand.to_csv("data/derived/demand_data.csv", index=False)

def main():
    # Create output directory
    Path("data/derived").mkdir(parents=True, exist_ok=True)
    
    # Prepare all data files
    prepare_products_data()
    prepare_warehouse_data()
    prepare_vendor_data()
    prepare_purchase_order_data()
    prepare_inventory_data()
    prepare_shipment_data()
    prepare_demand_data()
    
    print("All data preparation completed!")

if __name__ == "__main__":
    main()