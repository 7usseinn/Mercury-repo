CREATE TABLE IF NOT EXISTS bank_schema.agg_time_daily
(
    id  integer PRIMARY KEY NOT NULL,
    trans_date DATE NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (trans_date)
);

TRUNCATE TABLE bank_schema.agg_time_daily;

INSERT INTO bank_schema.agg_time_daily (id,trans_date, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
    id,
    CAST(trans_date_trans_time AS DATE) AS trans_date,
    COUNT(*) AS total_transactions,
    SUM(amt) AS total_amount,
    AVG(amt) AS average_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END) AS fraud_amount
FROM
    bank_schema.fct_fraud
GROUP BY
    trans_date;


CREATE TABLE IF NOT EXISTS bank_schema.agg_customer
(
    id  integer PRIMARY KEY NOT NULL,
    cc_num VARCHAR(255) NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (cc_num)
);

TRUNCATE TABLE bank_schema.agg_customer;

INSERT INTO bank_schema.agg_customer (id,cc_num, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
    id,
    cc_num,
    COUNT(*) AS total_transactions,
    SUM(amt) AS total_amount,
    AVG(amt) AS average_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END) AS fraud_amount
FROM
    bank_schema.fct_fraud
GROUP BY
    cc_num;


CREATE TABLE IF NOT EXISTS bank_schema.agg_merchant
(
    id  integer PRIMARY KEY NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (merchant)
);

TRUNCATE TABLE bank_schema.agg_merchant;

INSERT INTO bank_schema.agg_merchant (id,merchant, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
    id,
    merchant,
    COUNT(*) AS total_transactions,
    SUM(amt) AS total_amount,
    AVG(amt) AS average_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END) AS fraud_amount
FROM
    bank_schema.fct_fraud
GROUP BY
    merchant;


CREATE TABLE IF NOT EXISTS bank_schema.agg_state
(
    id  integer PRIMARY KEY NOT NULL,
    state VARCHAR(255) NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (state)
);

TRUNCATE TABLE bank_schema.agg_state;

INSERT INTO bank_schema.agg_state (id,state, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
    id,
    state,
    COUNT(*) AS total_transactions,
    SUM(amt) AS total_amount,
    AVG(amt) AS average_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END) AS fraud_amount
FROM
    bank_schema.fct_fraud
GROUP BY
    state;
