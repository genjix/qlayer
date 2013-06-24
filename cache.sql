DROP DOMAIN IF EXISTS address_type CASCADE;
CREATE DOMAIN address_type AS VARCHAR(34);
DROP DOMAIN IF EXISTS amount_type CASCADE;
CREATE DOMAIN amount_type AS NUMERIC(16, 8) CHECK (VALUE < 21000000 AND VALUE >= 0);
DROP DOMAIN IF EXISTS hash_type CASCADE;
CREATE DOMAIN hash_type AS bytea;

DROP TABLE IF EXISTS history;
DROP SEQUENCE IF EXISTS history_id_sequence;

CREATE SEQUENCE history_id_sequence;
CREATE TABLE history (
    id INT NOT NULL DEFAULT NEXTVAL('history_id_sequence') PRIMARY KEY,
    address address_type NOT NULL,
    output_point_hash hash_type NOT NULL,
    output_point_index INT NOT NULL,
    value amount_type NOT NULL,
    confirmed_credit BOOL NOT NULL,
    spend_point_hash hash_type,
    spend_point_index INT,
    confirmed_debit BOOL
);

CREATE INDEX ON history (address);
CREATE INDEX ON history (output_point_hash, output_point_index);

DROP TABLE IF EXISTS queue;

CREATE TABLE queue (
    address address_type NOT NULL UNIQUE,
    last_update_time TIMESTAMP
);

CREATE INDEX ON queue (address);

DROP TABLE IF EXISTS blocks;

CREATE TABLE blocks (
    depth INT NOT NULL UNIQUE,
    hash hash_type NOT NULL UNIQUE
);

CREATE INDEX ON blocks (depth);

