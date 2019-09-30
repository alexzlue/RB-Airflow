#standardSQL
SELECT unique_key,owning_department,source,status,created_date, close_date 
FROM `bigquery-public-data.austin_311.311_service_requests`;