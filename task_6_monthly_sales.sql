WITH Monthly_Sales AS (
    SELECT 
        d.year AS sales_year,
        d.month AS sales_month,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM 
        orders_table o
    JOIN 
        dim_products p ON o.product_code = p.product_code
    JOIN 
        dim_date_times d ON o.date_uuid = d.date_uuid
    GROUP BY 
        d.year,
        d.month
)
SELECT 
    sales_year,
    sales_month,
    ROUND(total_sales::numeric,2) AS total_sales
FROM 
    Monthly_Sales
ORDER BY 
    total_sales DESC

LIMIT 10;