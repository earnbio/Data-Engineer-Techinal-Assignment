    
# Data Engineer Technical Assignment

## Overview

This project implements an ETL pipeline in Python using Prefect for orchestration, SQLite as the source and target database, pandas for transformations, and pytest for unit testing.[web:586][web:587]

The pipeline extracts raw customer, order, and exchange rate data from `shopdata.db`, applies cleaning and enrichment rules, loads analytics-ready tables into SQLite, and falls back to CSV export if the database load fails.[web:586][web:587]

## Project Structure

```text
Data Engineer Technical Assignment/
├── data/
│   └── shopdata.db
│── Part 1 Data Exploration & Understanding (SQL)/
│   ├── exploration.sql
├── Part 2 Data Cleaning & Pipeline Orchestration (Python + Prefect)/
│   ├── pipeline.py
│   ├── analytics.db
│   ├── pipeline.ipynb
├── Part 3 Unit Testing (Python)/
│   ├── test_pipeline.py
│   ├── test_pipeline.ipynb
├── requirements.txt
└── README.md
```
## How to Run the Flow and Tests

### Run the flow

From the project root (`Data Engineer Technical Assignment/`):

```bash
python "Part 2 Data Cleaning & Pipeline Orchestration (Python + Prefect)/pipeline.py"

```

### Run all tests
From the project root, run (`Data Engineer Technical Assignment/`):

```bash
python -m pytest -v "Part 3 Unit Testing (Python)/test_pipeline.py"

```

## Findings summary

Customer Data Anomalies SQL Notes:
    
- `Duplicate customer `records were detected for the `customer_id` + `full_name` combination, including entries such as  (1, 'Alice Smith')
- `Duplicate customer` contact records were also found for the  `customer_id` + `full_name` + `phone`  combination, including  (2, 'Bob Jones', '555-987-6543') .
- Two records were identified with `missing  email`  values, and two additional records were found with `missing  phone  values`, indicating incomplete customer contact data.
- The  phone column contains phone numbers in multiple `inconsistent formats` (for example, some with country codes, some with dashes, and some with only digits).

Order Data Anomalies SQL Notes:

- Two records were identified with `missing currency values` (order_id 107 and 116), and one additional record were found with `missing order_date` values (order_id 117).
- two records were identified with `negative total_amount` values (order_id 103 with -50 and order_id 113 with -100)
- There are `mismatch in customer_id values` between the  `customers` table and the `orders` table for order numbers 106 and 118
   

  
  