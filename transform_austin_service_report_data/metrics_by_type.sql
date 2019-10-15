CREATE VIEW airflow.metrics_by_type AS
SELECT d1.complaint_type, 
       d1.total_reports AS total_reports,
       COALESCE(d2.reports_active,0) AS reports_active,
       COALESCE(d3.reports_closed,0) AS reports_closed,
       COALESCE(d4.reports_never_opened,0) AS reports_never_opened,
       COALESCE(d1.average_num_days_open,0) AS average_num_days_open
FROM
    --  total reports by complaint_type & avg time to complete
    (SELECT complaint_type AS complaint_type,
            COUNT(complaint_type) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
     FROM airflow.austin_service_reports
     GROUP BY complaint_type) d1
FULL JOIN
    -- total reports still opened 
    (SELECT complaint_type AS complaint_type,
            COUNT(complaint_type) AS reports_active
     FROM airflow.austin_service_reports
     WHERE close_date IS NULL
     GROUP BY complaint_type) d2
ON (d1.complaint_type=d2.complaint_type)
FULL JOIN
    -- total reports closed
    (SELECT complaint_type AS complaint_type,
            COUNT(complaint_type) AS reports_closed
     FROM airflow.austin_service_reports
     WHERE close_date IS NOT NULL
     GROUP BY complaint_type) d3
ON (d1.complaint_type=d3.complaint_type)
FULL JOIN
    -- total reports never opened
    (SELECT complaint_type AS complaint_type,
            COUNT(complaint_type) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY complaint_type) d4
ON (d1.complaint_type=d4.complaint_type)
ORDER BY d1.complaint_type;
