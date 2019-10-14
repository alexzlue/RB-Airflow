CREATE VIEW airflow.metrics_by_day AS
SELECT COALESCE(d1.date,d2.date) AS date, 
       COALESCE(d1.num_opened,0) AS num_opened, 
       COALESCE(d2.num_closed,0) AS num_closed, 
       COALESCE(d1.average_num_days_open,0) AS average_num_days_open,
       COALESCE(CAST((d4.num_unopened*100.00)/d1.num_opened AS DECIMAL(12, 2)),0) AS percent_unopened,
       COALESCE(CAST((d5.num_never_closed*100.00)/d1.num_opened AS DECIMAL(12, 2)),0) AS percent_never_closed
FROM
    -- collects number of reports created each day
    -- & collects average days to close each report
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(*) AS num_opened,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
    FROM airflow.austin_service_reports
    GROUP BY date) d1
FULL JOIN
    -- collects number of reports closed each day
    (SELECT CAST(close_date AS DATE) AS date,
            COUNT(*) AS num_closed
    FROM airflow.austin_service_reports
    WHERE close_date IS NOT NULL
    GROUP BY date) d2
ON (d1.date=d2.date)
FULL JOIN
    -- collects % of reports never opened that were created on each day
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(*) AS num_unopened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date
     GROUP BY date) d4
ON (d1.date=d4.date)
FULL JOIN
    -- collects % of reports never closed that were created on each day
    (SELECT CAST(created_date AS DATE) as date,
            COUNT(*) AS num_never_closed
     FROM airflow.austin_service_reports
     WHERE close_date IS NULL
     GROUP BY date) d5
ON (d1.date=d5.date)
ORDER BY d1.date;
