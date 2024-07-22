SELECT locality, COUNT(*) AS total_num_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_num_stores DESC