CREATE VIEW airflow.metrics_by_department AS
SELECT d1.department, 
       d1.total_reports AS total_reports,
       COALESCE(d2.reports_active,0) AS reports_active,
       COALESCE(d3.reports_closed,0) AS reports_closed,
       COALESCE(d4.reports_never_opened,0) AS reports_never_opened,
       COALESCE(d1.average_num_days_open,0) AS average_num_days_open
FROM
    --  total reports by department & avg time to complete
    (SELECT owning_department AS department,
            COUNT(*) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
     FROM airflow.austin_service_reports
     GROUP BY department) d1
FULL JOIN
    -- total reports still opened 
    (SELECT owning_department AS department,
            COUNT(*) AS reports_active
     FROM airflow.austin_service_reports
     WHERE close_date IS NULL
     GROUP BY department) d2
ON (d1.department=d2.department)
FULL JOIN
    -- total reports closed
    (SELECT owning_department AS department,
            COUNT(*) AS reports_closed
     FROM airflow.austin_service_reports
     WHERE close_date IS NOT NULL
     GROUP BY department) d3
ON (d1.department=d3.department)
FULL JOIN
    -- total reports never opened
    (SELECT owning_department AS department,
            COUNT(*) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY department) d4
ON (d1.department=d4.department)
ORDER BY d1.department;
