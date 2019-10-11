#standardSQL
SELECT unique_key,
    complaint_description,
    owning_department,
    source,
    status,
    created_date,
    last_update_date,
    close_date
FROM `bigquery-public-data.austin_311.311_service_requests`
WHERE (created_date >= '{0}') AND (created_date < '{1}');