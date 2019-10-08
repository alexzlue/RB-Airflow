CREATE SCHEMA IF NOT EXISTS airflow;

CREATE TABLE IF NOT EXISTS airflow.austin_service_reports (
    unique_key            char(12),
    complaint_description varchar(255),
    owning_department     varchar(255),
    source                varchar(255),
    status                varchar(255),
    created_date          timestamp,
    close_date            timestamp
);