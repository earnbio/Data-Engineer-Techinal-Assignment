WITH CLV AS (
    SELECT
        o.customer_id,
        c.full_name,
        -- cohort month-year from signup date 
        strftime('%Y-%m', c.signup_date) AS customer_cohort,
        -- count of valid orders
        COUNT(o.order_id) AS total_orders_placed,
        -- lifetime value: sum of valid USD amounts
        SUM(o.usd_amount) AS lifetime_value_usd
    FROM fct_orders AS o
    LEFT JOIN dim_customers AS c
        ON o.customer_id = c.customer_id
    GROUP BY
        o.customer_id,
        full_name,
        customer_cohort
)
SELECT
    customer_id,
    full_name,
    total_orders_placed,
    lifetime_value_usd,
    customer_cohort
FROM CLV
ORDER BY lifetime_value_usd DESC;

