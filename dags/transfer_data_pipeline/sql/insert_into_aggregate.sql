-- Collect reports found in the transfer table that 
-- don't exist in the aggregate table, and update 
-- outdated reports in aggregate that are in transfer
INSERT INTO airflow.aggregate_table
SELECT *
FROM airflow.transfer_table
ON CONFLICT (unique_key)
DO 
UPDATE SET 
    status=EXCLUDED.status,
    last_update_date=EXCLUDED.last_update_date,
    close_date=EXCLUDED.close_date
WHERE airflow.aggregate_table.last_update_date<EXCLUDED.last_update_date;