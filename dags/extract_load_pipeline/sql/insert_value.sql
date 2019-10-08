INSERT INTO airflow.austin_service_reports 
VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', {6})
ON CONFLICT DO NOTHING;