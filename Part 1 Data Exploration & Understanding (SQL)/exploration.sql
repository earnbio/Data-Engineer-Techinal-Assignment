

-- CHECK ANOMALIES FOR CUSTOMERS TABLE
SELECT *
FROM vw_raw_customers;

/*** Check for duplicates ***/
WITH
-- duplicates by customer_id
id_dups AS (
    SELECT COUNT(*) AS dup_rows
    FROM (
        SELECT customer_id
        FROM vw_raw_customers 
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    ) t
),
-- duplicates by fullname
name_dups AS (
    SELECT COUNT(*) AS dup_rows
    FROM (
        SELECT full_name
        FROM vw_raw_customers 
        GROUP BY full_name
        HAVING COUNT(*) > 1
    ) t
),
-- duplicates by email
email_dups AS (
    SELECT COUNT(*) AS dup_rows
    FROM (
        SELECT email
        FROM vw_raw_customers 
        GROUP BY email
        HAVING COUNT(*) > 1
    ) t
),
-- duplicates by phone
phone_dups AS (
    SELECT COUNT(*) AS dup_rows
    FROM (
        SELECT phone
        FROM vw_raw_customers 
        GROUP BY phone
        HAVING COUNT(*) > 1
    ) t
),
-- duplicates by signup_date
signup_dups AS (
    SELECT COUNT(*) AS dup_rows
    FROM (
        SELECT signup_date
        FROM vw_raw_customers 
        GROUP BY signup_date
        HAVING COUNT(*) > 1
    ) t
)
SELECT
    id_dups.dup_rows      AS customer_id_dup_values,
    name_dups.dup_rows    AS full_name_dup_values,
    email_dups.dup_rows   AS email_dup_values,
    phone_dups.dup_rows   AS phone_dup_values,
    signup_dups.dup_rows  AS signup_date_dup_values
FROM
    id_dups, name_dups, email_dups, phone_dups, signup_dups;

-- Duplicate by customer_id
SELECT customer_id, COUNT(*) AS cnt 
FROM vw_raw_customers
GROUP BY customer_id 
HAVING COUNT(*) > 1;

-- Find all rows where the customer_id is duplicated
SELECT *
FROM  vw_raw_customers vrc 
WHERE customer_id IN (1,2);

-- Find all rows where the full_name is duplicated
SELECT *
FROM  vw_raw_customers vrc 
WHERE full_name IN ("Alice Smith", "Bob Jones");

-- Duplicate by email
SELECT email, COUNT(*) AS cnt 
FROM vw_raw_customers
GROUP BY email
HAVING COUNT(*) > 1;

SELECT *
FROM  vw_raw_customers vrc 
WHERE email is null;

-- Duplicate by phone
SELECT phone, COUNT(*) AS cnt 
FROM vw_raw_customers
GROUP BY phone 
HAVING COUNT(*) > 1;

-- Find all rows where the phone is duplicated
SELECT *
FROM  vw_raw_customers 
WHERE phone IS "555-987-6543";

/*** Check missing values ***/
SELECT *
FROM vw_raw_customers
WHERE customer_id IS NULL
   OR full_name   IS NULL
   OR email       IS NULL
   OR signup_date IS NULL
   OR phone       IS NULL;

-- CHECK ANOMALIES FOR ORDER TABLE
SELECT *
FROM vw_raw_orders;

/*** Check for duplicates ***/
-- Duplicate order_id
SELECT
    order_id,
    COUNT(*) AS cnt
FROM vw_raw_orders 
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Duplicate combination of customer_id and order_date 
SELECT
    customer_id,
    order_date,
    COUNT(*) AS cnt
FROM vw_raw_orders 
GROUP BY customer_id, order_date
HAVING COUNT(*) > 1;

/*** Check missing values ***/
SELECT *
FROM vw_raw_orders 
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR order_date  IS NULL
   OR total_amount IS NULL
   OR currency IS NULL
   OR status IS NULL;

/*** Check Syntactical Anomalie ***/
-- Check date format
SELECT * 
FROM vw_raw_orders 
WHERE order_date NOT LIKE '2023-05%';

-- Check total_amount format
SELECT * 
FROM vw_raw_orders 
WHERE total_amount < 0;

SELECT DISTINCT status
FROM vw_raw_orders;

SELECT * 
FROM vw_raw_orders 
WHERE status LIKE 'SYSTEM_ERROR';

SELECT * 
FROM vw_raw_orders 
WHERE status LIKE 'CANCELLED';

SELECT * 
FROM vw_raw_orders 
WHERE status LIKE 'PENDING';

-- Check for foreign key discrepancies
SELECT
    c.customer_id   AS customer_table_id,
    o.order_id,
    o.customer_id   AS order_table_customer_id,
    o.order_date,
    o.total_amount
FROM vw_raw_customers AS c
FULL JOIN vw_raw_orders AS o
    ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;






