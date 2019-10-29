#standardSQL
SELECT unique_key,
    complaint_type,
    complaint_description,
    owning_department,
    source,
    status,
    created_date,
    last_update_date,
    close_date,
    city
FROM `bigquery-public-data.austin_311.311_service_requests`
WHERE (last_update_date >= '{0}') AND (last_update_date < '{1}');