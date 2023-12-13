Create Table if NOT EXISTS bank_schema.etl_watermark
(
    id SERIAL PRIMARY KEY NOT NULL,
    etl_last_execution_time TIMESTAMP
);

INSERT INTO bank_schema.etl_watermark (etl_last_execution_time)
VALUES ('1900-1-1 08:30:00');
