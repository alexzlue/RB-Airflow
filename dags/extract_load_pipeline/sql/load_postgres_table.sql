-- load from gcs file
COPY airflow.austin_service_reports 
FROM '{0}' 
DELIMITER '|' NULL '';

-- save outdated duplicates into backlog
WITH duplicates AS (
    SELECT unique_key, max(last_update_date) AS last_update_date, count(unique_key)
    FROM airflow.austin_service_reports
    GROUP BY unique_key
    HAVING count(unique_key)>1
)
INSERT INTO airflow.austin_service_reports_backlog (
    backlog_ts,
    unique_key,
    complaint_type,
    complaint_description,
    owning_department,
    source,
    status,
    created_date,
    last_update_date,
    close_date,
    city
)
SELECT '{1}' as backlog_ts,
    t1.*
FROM airflow.austin_service_reports t1, duplicates t2
where t1.unique_key=t2.unique_key AND t1.last_update_date<t2.last_update_date;

-- remove outdated duplicates
DELETE 
FROM airflow.austin_service_reports a 
    USING airflow.austin_service_reports b
WHERE a.unique_key=b.unique_key and a.last_update_date < b.last_update_date;
