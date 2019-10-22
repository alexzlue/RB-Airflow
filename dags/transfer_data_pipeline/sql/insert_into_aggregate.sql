-- Collect reports found in the transfer table that 
-- don't exist in the aggregate table
INSERT INTO airflow.aggregate_table
SELECT *
FROM (
    SELECT * FROM airflow.transfer_table
    EXCEPT
    SELECT * FROM airflow.aggregate_table
) AS difference;
