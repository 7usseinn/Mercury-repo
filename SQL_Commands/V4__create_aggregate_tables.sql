CREATE TABLE IF NOT EXISTS bank_schema.agg_time_daily
(
    trans_date DATE NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (trans_date)
);

INSERT INTO bank_schema.agg_time_daily (trans_date, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
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
    cc_num VARCHAR(255) NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (cc_num)
);

INSERT INTO bank_schema.agg_customer (cc_num, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
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
    merchant VARCHAR(255) NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (merchant)
);

INSERT INTO bank_schema.agg_merchant (merchant, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
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
    state VARCHAR(255) NOT NULL,
    total_transactions INTEGER,
    total_amount double precision,
    average_amount double precision,
    fraud_transactions INTEGER,
    fraud_amount double precision,
    PRIMARY KEY (state)
);

INSERT INTO bank_schema.agg_state (state, total_transactions, total_amount, average_amount, fraud_transactions, fraud_amount)
SELECT
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
