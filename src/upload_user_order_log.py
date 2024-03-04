"""
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

uol_url = "/lessons/project/old_report/user_order_log.csv"
df_uol = pd.read_csv(uol_url)
df_uol = df_uol.drop_duplicates(subset=['uniq_id'])
df_uol.reset_index(drop=True, inplace=True)

postgres_hook = PostgresHook('postgresql_de')
engine = postgres_hook.get_sqlalchemy_engine()
df_uol.to_sql(
    'staging.user_order_log', 
    con=engine, 
    if_exists='append', 
    index=False
)
"""
