SELECT 
    d.store_type,
    SUM(o.product_quantity * p.product_price) AS total_sales,
    (SUM(o.product_quantity * p.product_price) / SUM(SUM(o.product_quantity * p.product_price)) OVER ()) * 100 AS percentage_of_total_sales
FROM 
    orders_table o
JOIN 
    dim_store_details d ON o.store_code = d.store_code
JOIN 
    dim_products p ON o.product_code = p.product_code
GROUP BY 
    d.store_type;



