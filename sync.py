import pandas as pd
from sqlalchemy import create_engine, text
import os
import warnings
warnings.filterwarnings('ignore')

# ── All credentials from environment variables ────────────────────────────
DO_URL   = os.environ.get("DO_DB_URL")
SUPA_URL = os.environ.get("SUPABASE_DB_URL")

do_engine   = create_engine(DO_URL,   connect_args={"sslmode": "require"})
supa_engine = create_engine(SUPA_URL, connect_args={"sslmode": "require"})

def sync():
    print("Extracting from DigitalOcean...")

    dist   = pd.read_sql('SELECT * FROM "tc_distributor"', do_engine)
    orders = pd.read_sql('SELECT * FROM "order"', do_engine)
    line   = pd.read_sql('SELECT order_id, title, quantity FROM "line_item"', do_engine)

    orders = orders.rename(columns={'id':'order_id'})
    dist_small = dist[['id','business_name','state','city','is_active']]
    df = orders.merge(dist_small, left_on='distributor_id', right_on='id', how='left')
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    df['month'] = df['created_at'].dt.to_period('M').astype(str)

    metrics = df.groupby(['distributor_id','business_name','state','city']).agg(
        total_orders     = ('order_id', 'count'),
        paid_orders      = ('payment_status', lambda x: (x=='captured').sum()),
        fulfilled_orders = ('fulfillment_status', lambda x: (x=='fulfilled').sum()),
        canceled_orders  = ('status', lambda x: (x=='canceled').sum()),
        unique_customers = ('customer_id', 'nunique'),
    ).reset_index()
    metrics['fulfillment_rate'] = (metrics['fulfilled_orders'] / metrics['total_orders'] * 100).round(1)
    metrics['payment_rate']     = (metrics['paid_orders'] / metrics['total_orders'] * 100).round(1)
    metrics['updated_at']       = pd.Timestamp.now()

    monthly = df.groupby(['distributor_id','business_name','month']).agg(
        orders=('order_id','count')
    ).reset_index()
    monthly['updated_at'] = pd.Timestamp.now()

    top_products = line.merge(df[['order_id','business_name','distributor_id']], on='order_id')
    top_products = top_products.groupby(['distributor_id','business_name','title']).agg(
        total_qty=('quantity','sum')
    ).reset_index().sort_values('total_qty', ascending=False)
    top_products['updated_at'] = pd.Timestamp.now()

    print("Pushing to Supabase...")
    with supa_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dist_metrics CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dist_monthly CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dist_top_products CASCADE"))
        conn.commit()

    metrics.to_sql('dist_metrics', supa_engine, if_exists='replace', index=False)
    monthly.to_sql('dist_monthly', supa_engine, if_exists='replace', index=False)
    top_products.to_sql('dist_top_products', supa_engine, if_exists='replace', index=False)

    print(f"✓ Synced: {len(metrics)} distributors, {len(monthly)} monthly rows, {len(top_products)} products")

if __name__ == "__main__":
    sync()