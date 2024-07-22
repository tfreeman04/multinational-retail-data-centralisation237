SELECT user_uuid, COUNT(*)
FROM dim_users
GROUP BY user_uuid
HAVING COUNT(*) > 1;