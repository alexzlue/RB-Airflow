CREATE VIEW airflow.metrics_by_day AS
SELECT COALESCE(d1.date,d2.date) AS date, 
       COALESCE(d1.num_opened,0) AS num_opened, 
       COALESCE(d2.num_closed,0) AS num_closed, 
       COALESCE(d3.average_num_days_open,0) AS average_num_days_open
FROM
    -- collects number of reports created each day
    (SELECT CAST(created_date AS DATE) AS date,
            count(*) AS num_opened
    FROM airflow.austin_service_reports
    GROUP BY date) d1
FULL JOIN
    -- collects number of reports closed each day
    (SELECT CAST(close_date AS DATE) AS date,
            count(*) AS num_closed
    FROM airflow.austin_service_reports
    GROUP BY date) d2
ON (d1.date=d2.date)
FULL JOIN
    -- collects average days to close each report
    (SELECT CAST(created_date AS DATE) AS date, TRUNC(AVG(EXTRACT(DAY FROM close_date-created_date)),2) AS average_num_days_open
    FROM airflow.austin_service_reports
    GROUP BY date) d3
ON (d1.date=d3.date)
ORDER BY d1.date;
