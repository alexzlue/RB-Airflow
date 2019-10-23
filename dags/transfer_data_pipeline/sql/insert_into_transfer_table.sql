-- insertion into
INSERT INTO airflow.transfer_table
SELECT *
FROM airflow.austin_service_reports
WHERE (last_update_date >= '{0}') AND (last_update_date < '{1}');

-- remove outdated duplicates
DELETE 
FROM airflow.transfer_table a 
    USING airflow.transfer_table b
WHERE a.unique_key=b.unique_key and a.last_update_date < b.last_update_date;
