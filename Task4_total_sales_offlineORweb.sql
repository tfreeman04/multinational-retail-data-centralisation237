SELECT 
    COUNT(orders_table.index) AS number_of_sales, 
    SUM(orders_table.product_quantity) AS product_quantity_count, 
    CASE 
        WHEN dim_store_details.store_type ILIKE '%web%' THEN 'Web' 
        ELSE 'Offline' 
    END AS location
FROM orders_table,dim_store_details
GROUP BY 
    CASE 
        WHEN store_type ILIKE '%web%' THEN 'Web' 
        ELSE 'Offline' 
    END;
