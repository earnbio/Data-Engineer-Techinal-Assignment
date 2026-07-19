#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
from prefect.logging import disable_run_logger

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent  # job assignment
PART2_PATH = PROJECT_ROOT / "Part 2 Data Cleaning & Pipeline Orchestration (Python + Prefect)"

sys.path.insert(0, str(PART2_PATH))

from pipeline import transform_customers, transform_orders


def test_transform_customers_with_dummy_data():
    input_df = pd.DataFrame(
        {
            "customer_id": [1, 1, 2, 3, 3, 4, 5, 6],
            "name": ["Alice","Alice","Bob","Charlie","Charlie","Diana","Evan","Fiona",],
            "email": [None,"alice@example.com",None,"charlie@old.com",None,"diana@example.com",
                      None,"fiona@example.com",],
            "phone": ["(040) 123-4567","0401 999 888","+61 412-345-678","03-9999-8888","(03) 7777 6666",
                      None,"abc-0400-555-111","",],
            "signup_date": ["2024-01-10","2024-02-15","2024-03-01","2024-01-05","2024-04-20","2024-02-11",
                            "2024-05-01","2024-06-01",],
        }
    )
    
    expected = pd.DataFrame(
        {
            "customer_id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice","Bob","Charlie","Diana","Evan","Fiona",],
            "email": ["alice@example.com","unknown@domain.com","unknown@domain.com","diana@example.com",
                      "unknown@domain.com","fiona@example.com",],
            "phone": ["0401999888","61412345678","0377776666",None,"0400555111",None,],
            "signup_date": [
                pd.to_datetime("2024-02-15").date(),
                pd.to_datetime("2024-03-01").date(),
                pd.to_datetime("2024-04-20").date(),
                pd.to_datetime("2024-02-11").date(),
                pd.to_datetime("2024-05-01").date(),
                pd.to_datetime("2024-06-01").date(),
            ],
        }
    )

    with disable_run_logger():
        result = transform_customers.fn(input_df).reset_index(drop=True)

    result = result.where(pd.notna(result), None)
    assert_frame_equal(result, expected)
    
def test_transform_orders_with_dummy_data():
    orders_df = pd.DataFrame(
        {
            "order_id": [101, 102, 103, 104, 105, 106, 107, 108],
            "customer_id": [1, 2, 3, 4, 5, 6, 1, 2],
            "order_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03",
                "2024-01-04", "2024-01-04", "2024-01-05",],
            "currency": ["EUR", "USD", None, "GBP", "AUD", "EUR", "JPY", "USD"],
            "total_amount": [100.0, -50.0, 200.0, 80.0, 150.0, 120.0, 300.0, 0.0],
        }
    )

    exchange_rates_df = pd.DataFrame(
        {
            "currency": ["EUR", "GBP", "AUD"],
            "date": ["2024-01-01", "2024-01-03", "2024-01-03"],
            "rate_to_usd": [1.1, 1.3, 0.7],
        }
    )

    expected = pd.DataFrame(
        {
            "order_id": [101, 103, 104, 105, 106, 107],
            "customer_id": [1, 3, 4, 5, 6, 1],
            "order_date": [
                pd.to_datetime("2024-01-01").date(),
                pd.to_datetime("2024-01-02").date(),
                pd.to_datetime("2024-01-03").date(),
                pd.to_datetime("2024-01-03").date(),
                pd.to_datetime("2024-01-04").date(),
                pd.to_datetime("2024-01-04").date(),
            ],
            "usd_amount": [110.0, 200.0, 104.0, 105.0, 120.0, 300.0],
        }
    )

    with disable_run_logger():
        result = transform_orders.fn(orders_df, exchange_rates_df).reset_index(drop=True)

    assert_frame_equal(result, expected)

def test_transform_orders_missing_rate_defaults_to_one():
    orders_df = pd.DataFrame(
        {
            "order_id": [201, 202, 203],
            "customer_id": [10, 11, 12],
            "order_date": ["2024-05-01", "2024-05-01", "2024-05-02"],
            "currency": ["CAD", "USD", None],
            "total_amount": [90.0, 50.0, 70.0],
        }
    )

    exchange_rates_df = pd.DataFrame(
        {
            "currency": ["EUR"],
            "date": ["2024-05-01"],
            "rate_to_usd": [1.2],
        }
    )

    expected = pd.DataFrame(
        {
            "order_id": [201, 202, 203],
            "customer_id": [10, 11, 12],
            "order_date": [
                pd.to_datetime("2024-05-01").date(),
                pd.to_datetime("2024-05-01").date(),
                pd.to_datetime("2024-05-02").date(),
            ],
            "usd_amount": [90.0, 50.0, 70.0],
        }
    )

    with disable_run_logger():
        result = transform_orders.fn(orders_df, exchange_rates_df).reset_index(drop=True)

    assert_frame_equal(result, expected)

    

