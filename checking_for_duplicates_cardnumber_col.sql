SELECT card_number, COUNT(*)
FROM dim_card_details
GROUP BY card_number
HAVING COUNT(*) > 1;