CREATE VIEW airflow.metrics_by_city AS
SELECT COALESCE(d1.city,'NULL') AS city, 
       d1.total_reports,
       COALESCE(d2.reports_active,0) AS reports_active,
       COALESCE(d3.reports_closed,0) AS reports_closed,
       COALESCE(d4.reports_never_opened,0) AS reports_never_opened,
       COALESCE(d1.average_num_days_open,0) AS average_num_days_open
FROM
    --  total reports by city & avg time to complete
    (SELECT UPPER(city) as city,
            COUNT(*) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
     FROM airflow.austin_service_reports
     GROUP BY UPPER(city)) d1
FULL JOIN
    -- total reports still opened 
    (SELECT UPPER(city) as city,
            COUNT(*) AS reports_active
     FROM airflow.austin_service_reports
     WHERE close_date IS NULL
     GROUP BY UPPER(city)) d2
ON (d1.city=d2.city)
FULL JOIN
    -- total reports closed
    (SELECT UPPER(city) as city,
            COUNT(*) AS reports_closed
     FROM airflow.austin_service_reports
     WHERE close_date IS NOT NULL
     GROUP BY UPPER(city)) d3
ON (d1.city=d3.city)
FULL JOIN
    -- total reports never opened
    (SELECT UPPER(city) as city,
            COUNT(*) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY UPPER(city)) d4
ON (d1.city=d4.city)
ORDER BY d1.city;
