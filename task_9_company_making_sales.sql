WITH FullTimestamps AS (
    SELECT
        o.index,
        -- Combine year, month, day, and time into a full timestamp
        (CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp))::timestamp AS full_timestamp,
        -- Use LEAD to get the next sale timestamp
        LEAD((CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp))::timestamp) 
            OVER (PARTITION BY EXTRACT(YEAR FROM (CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp))::timestamp) 
                  ORDER BY (CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp))::timestamp) AS next_sale_timestamp,
        EXTRACT(YEAR FROM (CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp))::timestamp) AS sale_year
    FROM
        orders_table o
    JOIN
        dim_date_times d ON o.date_uuid = d.date_uuid
),
TimeDiffs AS (
    SELECT
        sale_year,
        EXTRACT(EPOCH FROM (next_sale_timestamp - full_timestamp)) AS time_diff_seconds
    FROM
        FullTimestamps
    WHERE
        next_sale_timestamp IS NOT NULL
),
AggregatedTimeDiffs AS (
    SELECT
        sale_year,
        AVG(time_diff_seconds) AS avg_time_diff_seconds
    FROM
        TimeDiffs
    GROUP BY
        sale_year
)
SELECT
    sale_year,
    CONCAT(
        '"hours": ', FLOOR(avg_time_diff_seconds / 3600), ', ',
        '"minutes": ', FLOOR((avg_time_diff_seconds % 3600) / 60), ', ',
        '"seconds": ', FLOOR(avg_time_diff_seconds % 60), ', ',
        '"milliseconds": ', ROUND((avg_time_diff_seconds - FLOOR(avg_time_diff_seconds)) * 1000)
    ) AS time_details
FROM
    AggregatedTimeDiffs
ORDER BY
    sale_year;
