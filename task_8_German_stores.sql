SELECT 
    ds.store_type,
    ROUND(SUM(CAST(o.product_quantity * p.product_price AS numeric)), 2) AS total_sales,
	ds.country_code
FROM 
    orders_table o
JOIN 
    dim_store_details ds ON o.store_code = ds.store_code
JOIN 
    dim_products p ON o.product_code = p.product_code
WHERE 
    ds.country_code = 'DE'
GROUP BY 
    ds.country_code, ds.store_type
ORDER BY 
    total_sales DESC;


