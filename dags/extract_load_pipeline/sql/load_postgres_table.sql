-- load from gcs file
COPY airflow.austin_service_reports 
FROM '{0}' 
DELIMITER '|' NULL '';

-- remove outdated duplicates
DELETE 
FROM airflow.austin_service_reports a 
    USING airflow.austin_service_reports b
WHERE a.unique_key=b.unique_key and a.last_update_date < b.last_update_date;
