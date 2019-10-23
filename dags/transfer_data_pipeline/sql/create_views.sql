-- metrics by city
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


-- metrics by day
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
            COUNT(created_date) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
    FROM airflow.austin_service_reports
    GROUP BY date) d1
FULL JOIN
    -- collects number of reports closed for reports made for each day
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(created_date) AS reports_closed
    FROM airflow.austin_service_reports
    WHERE close_date IS NOT NULL
    GROUP BY date) d2
ON (d1.date=d2.date)
FULL JOIN
    -- collects number of ongoing reports from each day
    -- can be used to calculate % never closed
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(created_date) AS reports_active
    FROM airflow.austin_service_reports
    WHERE close_date is NULL
    GROUP BY date) d3
ON (d1.date=d3.date)
FULL JOIN
    -- collects # of reports never opened that were created on each day
    (SELECT CAST(created_date AS DATE) AS date,
            COUNT(created_date) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY date) d4
ON (d1.date=d4.date)
ORDER BY d1.date;


-- metrics by department
CREATE VIEW airflow.metrics_by_department AS
SELECT d1.department, 
       d1.total_reports,
       COALESCE(d2.reports_active,0) AS reports_active,
       COALESCE(d3.reports_closed,0) AS reports_closed,
       COALESCE(d4.reports_never_opened,0) AS reports_never_opened,
       COALESCE(d1.average_num_days_open,0) AS average_num_days_open
FROM
    --  total reports by department & avg time to complete
    (SELECT owning_department AS department,
            COUNT(owning_department) AS total_reports,
            CAST(AVG(EXTRACT(DAY FROM close_date-created_date)) AS DECIMAL (12,2)) AS average_num_days_open
     FROM airflow.austin_service_reports
     GROUP BY department) d1
FULL JOIN
    -- total reports still opened 
    (SELECT owning_department AS department,
            COUNT(owning_department) AS reports_active
     FROM airflow.austin_service_reports
     WHERE close_date IS NULL
     GROUP BY department) d2
ON (d1.department=d2.department)
FULL JOIN
    -- total reports closed
    (SELECT owning_department AS department,
            COUNT(owning_department) AS reports_closed
     FROM airflow.austin_service_reports
     WHERE close_date IS NOT NULL
     GROUP BY department) d3
ON (d1.department=d3.department)
FULL JOIN
    -- total reports never opened
    (SELECT owning_department AS department,
            COUNT(owning_department) AS reports_never_opened
     FROM airflow.austin_service_reports
     WHERE created_date=last_update_date AND close_date is NULL
     GROUP BY department) d4
ON (d1.department=d4.department)
ORDER BY d1.department;


-- metrics by source
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


-- metrics by type
CREATE VIEW airflow.metrics_by_type AS
SELECT d1.complaint_type, 
       d1.total_reports,
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
