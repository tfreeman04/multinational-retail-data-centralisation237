-- Update opening_date in dim_store_details correcting multiple miss-typed dates
UPDATE dim_store_details
SET opening_date = 
    CASE 
        WHEN opening_date ~ '^[A-Za-z]+\s+\d{4}\s+\d{2}$' THEN TO_DATE(opening_date, 'Month YYYY DD')::DATE
        WHEN opening_date ~ '^[A-Za-z]+\s+\d{4}$' THEN TO_DATE(opening_date, 'Month YYYY')::DATE
        ELSE opening_date::DATE  -- Keep existing value if no match
    END
WHERE 
    opening_date IS NOT NULL
    AND opening_date ~ '^[A-Za-z]+\s+\d{4}(\s+\d{2})?$';
