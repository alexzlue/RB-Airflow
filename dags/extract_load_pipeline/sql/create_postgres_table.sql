CREATE SCHEMA IF NOT EXISTS airflow;

CREATE TABLE IF NOT EXISTS airflow.austin_service_reports (
    unique_key            char(11),
    complaint_type        varchar(15),
    complaint_description varchar(255),
    owning_department     varchar(255),
    source                varchar(255),
    status                varchar(255),
    created_date          timestamp,
    last_update_date      timestamp,
    close_date            timestamp,
    city                  varchar(50)
);

CREATE TABLE IF NOT EXISTS airflow.austin_service_reports_backlog (
    id                    SERIAL,
    backlog_ts            timestamp,
    unique_key            char(11),
    complaint_type        varchar(15),
    complaint_description varchar(255),
    owning_department     varchar(255),
    source                varchar(255),
    status                varchar(255),
    created_date          timestamp,
    last_update_date      timestamp,
    close_date            timestamp,
    city                  varchar(50),
    PRIMARY KEY           (id)
);
