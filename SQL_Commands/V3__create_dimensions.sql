CREATE Table IF not EXISTS bank_schema.dim_orig_dest
(
      id SERIAL PRIMARY KEY NOT NULL,
      nameorig VARCHAR(255),
      namedest VARCHAR(255)
);

CREATE Table IF not EXISTS bank_schema.fact_orig_dest
(
      id SERIAL PRIMARY KEY NOT NULL,
      type VARCHAR(255),
      amount FLOAT,
      oldbalanceorig FLOAT,
      newbalanceorig FLOAT,
      oldbalancedest FLOAT,
      newbalancedest FLOAT,
      isfraud INT
);