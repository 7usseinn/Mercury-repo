CREATE Table IF not EXISTS bank_schema.fct_fraud
(
    id  integer PRIMARY KEY NOT NULL,
    trans_date_trans_time  VARCHAR(255),
    cc_num VARCHAR(255),
    merchant VARCHAR(255),
    category VARCHAR(255),
    amt double precision,
    first VARCHAR(255),
    last VARCHAR(255),
    gender VARCHAR(255),
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip integer,
    lat double precision,
    long double precision,
    city_pop double precision,
    job VARCHAR(255),
    dob VARCHAR(255),
    trans_num VARCHAR(255),
    unix_time double precision,
    merch_lat double precision,
    merch_long double precision,
    is_fraud integer
);

ALTER TABLE bank_schema.fct_fraud ADD CONSTRAINT unique_trans_num UNIQUE (trans_num);

INSERT INTO bank_schema.fct_fraud (id, trans_date_trans_time, cc_num, merchant, category, amt, first, last, gender, street, city, state, zip, lat, long, city_pop, job, dob, trans_num, unix_time, merch_lat, merch_long, is_fraud)
SELECT * FROM bank_schema.stg_kaggle_fraud
ON CONFLICT (trans_num) 
DO UPDATE SET 
    trans_date_trans_time = EXCLUDED.trans_date_trans_time,
    cc_num = EXCLUDED.cc_num,
    merchant = EXCLUDED.merchant,
    category = EXCLUDED.category,
    amt = EXCLUDED.amt,
    first = EXCLUDED.first,
    last = EXCLUDED.last,
    gender = EXCLUDED.gender,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip = EXCLUDED.zip,
    lat = EXCLUDED.lat,
    long = EXCLUDED.long,
    city_pop = EXCLUDED.city_pop,
    job = EXCLUDED.job,
    dob = EXCLUDED.dob,
    unix_time = EXCLUDED.unix_time,
    merch_lat = EXCLUDED.merch_lat,
    merch_long = EXCLUDED.merch_long,
    is_fraud = EXCLUDED.is_fraud;
