#!/usr/bin/env python
# coding: utf-8

# In[18]:


from pathlib import Path
import sqlite3
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

PROJECT_ROOT = BASE_DIR.parent
DB_PATH = PROJECT_ROOT/"data"/"shopdata.db"

OUTPUT_DB_PATH = BASE_DIR/"analytics.db"
CLEAN_CUSTOMERS_CSV = BASE_DIR /"clean_customers.csv"
CLEAN_ORDERS_CSV = BASE_DIR/"clean_orders.csv"


@task(retries=2, retry_delay_seconds=5)
def extract(db_path: str) -> dict:
    logger = get_run_logger()
    logger.info(f"Starting extract from {db_path}")

    try:
        tables = ["vw_raw_customers", "vw_raw_orders", "vw_exchange_rates"]
        extracted_data = {}

        with sqlite3.connect(db_path) as conn:
            for table in tables:
                query = f"SELECT * FROM {table}"
                df = pd.read_sql_query(query, conn)
                extracted_data[table] = df
                logger.info(f"Loaded {len(df)} rows from {table}")

        logger.info("Extract completed successfully")
        return extracted_data

    except Exception:
        logger.exception("Extract failed")
        raise
        
@task
def transform_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Starting customer transform with {len(customers_df)} rows")

    try:
        customers_df = customers_df.copy()

        customers_df["signup_date"] = pd.to_datetime(
            customers_df["signup_date"], errors="coerce"
        ).dt.date

        before_dedup = len(customers_df)
        customers_df = customers_df.sort_values(
            by=["customer_id", "signup_date"],
            ascending=[True, False]
        )
        customers_df = customers_df.drop_duplicates(
            subset=["customer_id"],
            keep="first"
        )
        after_dedup = len(customers_df)
        logger.info(
            f"Customer dedup complete: removed {before_dedup - after_dedup} duplicate rows"
        )

        phone_mask = customers_df["phone"].notna()
        customers_df.loc[phone_mask, "phone"] = (
            customers_df.loc[phone_mask, "phone"]
            .astype(str)
            .str.replace(r"\D+", "", regex=True)
        )
        customers_df["phone"] = customers_df["phone"].replace("", pd.NA)

        missing_email_count = customers_df["email"].isna().sum()
        customers_df["email"] = customers_df["email"].fillna("unknown@domain.com")
        logger.info(f"Filled {missing_email_count} missing email values")

        logger.info(f"Customer transform completed with {len(customers_df)} rows")
        return customers_df

    except Exception:
        logger.exception("Customer transform failed")
        raise

@task
def transform_orders(
    orders_df: pd.DataFrame,
    exchange_rates_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Starting order transform with {len(orders_df)} rows")

    try:
        orders_df = orders_df.copy()
        exchange_rates_df = exchange_rates_df.copy()

        before_filter = len(orders_df)
        orders_df = orders_df[orders_df["total_amount"] > 0].copy()
        after_filter = len(orders_df)
        logger.info(
            f"Filtered out {before_filter - after_filter} orders with total_amount <= 0"
        )

        orders_df["order_date"] = pd.to_datetime(
            orders_df["order_date"], errors="coerce"
        ).dt.date
        exchange_rates_df["date"] = pd.to_datetime(
            exchange_rates_df["date"], errors="coerce"
        ).dt.date

        orders_with_fx = orders_df.merge(
            exchange_rates_df,
            left_on=["currency", "order_date"],
            right_on=["currency", "date"],
            how="left",
        )
        logger.info(f"Merged exchange rates; resulting rows: {len(orders_with_fx)}")

        missing_currency_count = orders_with_fx["currency"].isna().sum()
        orders_with_fx["currency"] = orders_with_fx["currency"].fillna("USD")
        orders_with_fx.loc[
            orders_with_fx["currency"] == "USD", "rate_to_usd"
        ] = 1.0

        missing_rate_count = orders_with_fx["rate_to_usd"].isna().sum()
        orders_with_fx["rate_to_usd"] = orders_with_fx["rate_to_usd"].fillna(1.0)

        logger.info(
            f"Applied USD defaults for {missing_currency_count} missing currencies and {missing_rate_count} missing rates"
        )

        orders_with_fx["usd_amount"] = (
            orders_with_fx["total_amount"] * orders_with_fx["rate_to_usd"]
        )

        orders_with_fx.drop(
            ["rate_to_usd", "date", "currency", "total_amount"],
            axis=1,
            inplace=True,
        )

        cols = list(orders_with_fx.columns)
        cols.remove("usd_amount")
        target_idx = cols.index("order_date") + 1
        cols.insert(target_idx, "usd_amount")
        orders_with_fx = orders_with_fx[cols]

        logger.info(f"Order transform completed with {len(orders_with_fx)} rows")
        return orders_with_fx

    except Exception:
        logger.exception("Order transform failed")
        raise

@task(retries=1, retry_delay_seconds=3)
def load_to_sqlite(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    db_path: str = str(OUTPUT_DB_PATH)) -> None:
    logger = get_run_logger()
    logger.info(f"Starting load to SQLite database at {db_path}")

    try:
        with sqlite3.connect(db_path) as conn:
            customers_df.to_sql(
                "dim_customers",
                conn,
                if_exists="replace",
                index=False,
            )
            orders_df.to_sql(
                "fct_orders",
                conn,
                if_exists="replace",
                index=False,
            )

        logger.info(
            f"Load to SQLite completed: dim_customers={len(customers_df)} rows, fct_orders={len(orders_df)} rows"
        )

    except Exception:
        logger.exception("Load to SQLite failed")
        raise
        
@task
def load_to_csv(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    customers_path: str = str(CLEAN_CUSTOMERS_CSV),
    orders_path: str = str(CLEAN_ORDERS_CSV)
) -> None:
    logger = get_run_logger()
    logger.info("Starting CSV fallback export")

    try:
        customers_df.to_csv(customers_path, index=False)
        orders_df.to_csv(orders_path, index=False)
        logger.info(
            f"CSV fallback completed: {customers_path}, {orders_path}"
        )

    except Exception:
        logger.exception("CSV fallback export failed")
        raise

@flow(name="etl-pipeline")
def etl_pipeline(db_path: str = str(DB_PATH)) -> None:
    logger = get_run_logger()
    logger.info("ETL pipeline started")

    try:
        data = extract(db_path)
        
        customers_df = transform_customers(data["vw_raw_customers"])
        orders_df = transform_orders(
            data["vw_raw_orders"],
            data["vw_exchange_rates"],
        )
        
        try:
            load_to_sqlite(customers_df, orders_df)
        except Exception:
            logger.exception("Primary SQLite load failed; attempting CSV fallback")
            load_to_csv(customers_df, orders_df)

        logger.info("ETL pipeline completed successfully")

    except Exception:
        logger.exception("ETL pipeline failed")
        raise


if __name__ == "__main__":
    etl_pipeline()


# In[ ]:




