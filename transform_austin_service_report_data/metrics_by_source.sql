CREATE VIEW airflow.metrics_by_source AS
SELECT CASE 
            WHEN d1.source='Phone' THEN 'Phone'
            WHEN d1.source='Spot311 Interface' THEN 'Spot311 Interface'
            ELSE 'Other'
       END AS report_source, 
       SUM(d1.total_reports) as total_reports,
       SUM(COALESCE(d2.reports_active,0)) AS reports_active,
       SUM(COALESCE(d3.reports_closed,0)) AS reports_closed,
       SUM(COALESCE(d4.reports_never_opened,0)) AS reports_never_opened,
       SUM(COALESCE(d1.average_num_days_open,0)) AS average_num_days_open
FROM
    --  total reports by source & avg time to complete
    (SELECT source AS source,
            COUNT(source) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
     FROM airflow.austin_service_reports
     GROUP BY source) d1
FULL JOIN
    -- total reports still opened 
    (SELECT source AS source,
            COUNT(source) AS reports_active
     FROM airflow.austin_service_reports
     WHERE close_date IS NULL
     GROUP BY source) d2
ON (d1.source=d2.source)
FULL JOIN
    -- total reports closed
    (SELECT source AS source,
            COUNT(source) AS reports_closed
     FROM airflow.austin_service_reports
     WHERE close_date IS NOT NULL
     GROUP BY source) d3
ON (d1.source=d3.source)
FULL JOIN
    -- total reports never opened
    (SELECT source AS source,
            COUNT(source) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY source) d4
ON (d1.source=d4.source)
GROUP BY report_source
ORDER BY report_source DESC;
