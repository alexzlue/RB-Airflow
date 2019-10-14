CREATE VIEW airflow.metrics_by_day AS
SELECT d1.date,
       d1.total_reports,
       COALESCE(d3.reports_active,0) AS reports_active, 
       COALESCE(d2.reports_closed,0) AS reports_closed,
       COALESCE(d4.reports_never_opened,0) as reports_never_opened,
       COALESCE(d1.average_num_days_open,0) AS average_num_days_open
FROM
    -- collects number of reports closed for reports made for each day
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(*) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
    FROM airflow.austin_service_reports
    GROUP BY date) d1
FULL JOIN
    -- collects number of reports closed for reports made for each day
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(*) AS reports_closed
    FROM airflow.austin_service_reports
    WHERE close_date IS NOT NULL
    GROUP BY date) d2
ON (d1.date=d2.date)
FULL JOIN
    -- collects number of ongoing reports from each day
    -- can be used to calculate % never closed
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(*) AS reports_active
    FROM airflow.austin_service_reports
    WHERE close_date is NULL
    GROUP BY date) d3
ON (d1.date=d3.date)
FULL JOIN
    -- collects # of reports never opened that were created on each day
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(*) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY date) d4
ON (d1.date=d4.date)
ORDER BY d1.date;
