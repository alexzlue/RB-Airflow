INSERT INTO airflow.transfer_table
SELECT *
FROM airflow.austin_service_reports
WHERE (created_date >= '{0}') AND (created_date < '{1}')