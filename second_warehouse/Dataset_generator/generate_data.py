import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set seed for reproducibility
np.random.seed(42)
rows = 1000

# 1. Generate Orders
regions = ['EMEA', 'APAC', 'NORTH_AMERICA', 'LATAM', 'europe'] # 'europe' is a dirty record
orders = pd.DataFrame({
    'order_id': range(1001, 1001 + rows),
    'customer_id': np.random.randint(5000, 6000, size=rows),
    'order_at': [datetime(2026, 1, 1) + timedelta(hours=x) for x in range(rows)],
    'region': np.random.choice(regions, size=rows),
    'total_usd': np.random.uniform(20.0, 500.0, size=rows).round(2)
})

# Add duplicates for Order ID 1050 to 1060 to test deduplication
duplicates = orders.iloc[50:61].copy()
orders = pd.concat([orders, duplicates], ignore_index=True)

# 2. Generate Shipments
carriers = ['DHL', 'FEDEX', 'UPS', 'BLUE_DART']
shipments = pd.DataFrame({
    'shipment_id': range(9001, 9001 + len(orders)),
    'order_id': orders['order_id'],
    'carrier': np.random.choice(carriers, size=len(orders)),
    'shipped_at': orders['order_at'] + pd.to_timedelta(np.random.randint(6, 48, size=len(orders)), unit='h')
})

# Delivery logic: 5% are NULL (lost), others delivered 1-5 days after shipping
shipments['delivered_at'] = shipments['shipped_at'] + pd.to_timedelta(np.random.randint(24, 120, size=len(orders)), unit='h')
shipments.loc[np.random.choice(shipments.index, size=50, replace=False), 'delivered_at'] = np.nan
shipments['shipping_cost'] = np.random.uniform(5.0, 30.0, size=len(orders)).round(2)

# Save to CSV
orders.to_csv('raw_orders.csv', index=False)
shipments.to_csv('raw_shipments.csv', index=False)

print("✅ Success: raw_orders.csv and raw_shipments.csv generated with 1000+ rows.")