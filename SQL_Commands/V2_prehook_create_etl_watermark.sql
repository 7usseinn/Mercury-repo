Create Table if NOT EXISTS bank_schema.etl_watermark
(
    id SERIAL PRIMARY KEY NOT NULL,
    etl_last_execution_time TIMESTAMP
);