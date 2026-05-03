
--CREATE DATABASE DE2026_Kuznetsova;

CREATE DATABASE "DE2026_Kuznetsova";


CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS meta;



CREATE TABLE IF NOT EXISTS meta.load_log (
	id serial4 NOT NULL,
	table_name varchar(100) NULL,
	load_start timestamp NULL,
	load_end timestamp NULL,
	rows_loaded int4 NULL,
	status varchar(20) NULL,
	CONSTRAINT load_log_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.deal (
	id int4 NULL,
	datetime timestamptz NULL,
	amount numeric NULL,
	status int4 NULL,
	plan_service_date timestamptz NULL,
	city_ccode varchar(3) NULL,
	loaded_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	hash_key varchar(64) NULL
);

CREATE TABLE IF NOT EXISTS stg.expense (
	id int4 NULL,
	datetime timestamptz NULL,
	amount numeric NULL,
	category text NULL,
	loaded_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS stg.income (
	id int4 NULL,
	amount numeric NULL,
	tauschpartner text NULL,
	city int4 NULL,
	deal_id int4 NULL,
	"date" varchar(50) NULL,
	ddate date NULL,
	loaded_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_city (
	id int4 NOT NULL,
	ccode varchar(3) NULL,
	nname varchar(50) NULL,
	CONSTRAINT dim_city_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS dwh.dim_date (
	id int4 NOT NULL,
	date_dat date NULL,
	date_month date NULL,
	date_quarter date NULL,
	date_year date NULL,
	CONSTRAINT dim_date_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_operation (
	id int4 NOT NULL,
	nname varchar(50) NULL,
	CONSTRAINT dim_operation_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dwh.fact_cashflow (
	hash_key varchar(64) NOT NULL,
	cashflow_id int4 NOT NULL,
	operation_id int4 NOT NULL,
	city_id int4 NULL,
	deal_id int4 NULL,
	date_id int4 NULL,
	amount numeric(15, 2) NULL,
	CONSTRAINT fact_cashflow_pkey PRIMARY KEY (hash_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_deal (
	hash_key varchar(64) NOT NULL,
    deal_id int4 NOT NULL,
	date_id int4 NOT NULL,
	city_id int4 NULL,
	plan_date_id int4 NULL,
	amount numeric(15, 2) NULL,
	CONSTRAINT fact_deal_pkey PRIMARY KEY (hash_key)
);


