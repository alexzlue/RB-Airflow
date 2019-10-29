CREATE TABLE IF NOT EXISTS airflow.transfer_table (
    unique_key            char(12),
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

CREATE TABLE IF NOT EXISTS airflow.aggregate_table (
    unique_key            char(12),
    complaint_type        varchar(15),
    complaint_description varchar(255),
    owning_department     varchar(255),
    source                varchar(255),
    status                varchar(255),
    created_date          timestamp,
    last_update_date      timestamp,
    close_date            timestamp,
    city                  varchar(50),
    PRIMARY KEY           (unique_key)
);
