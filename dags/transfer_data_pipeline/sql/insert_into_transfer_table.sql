-- insertion into
INSERT INTO airflow.transfer_table
SELECT *
FROM airflow.austin_service_reports
WHERE (last_update_date >= '{0}') AND (last_update_date < '{1}');
