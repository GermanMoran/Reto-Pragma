
CREATE SCHEMA IF NOT EXISTS BT;


CREATE TABLE IF NOT EXISTS BT.transactions (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            user_id INTEGER NOT NULL,
            source_file TEXT NOT NULL
        );

CREATE TABLE IF NOT EXISTS BT.ingestion_log (
    file_name   TEXT PRIMARY KEY,
    rows_loaded INTEGER NOT NULL,
    loaded_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS BT.stats (
    name TEXT PRIMARY KEY CHECK (name = 'global'),
    cnt  BIGINT NOT NULL,
    ssum DOUBLE PRECISION,
    smin DOUBLE PRECISION,
    smax DOUBLE PRECISION
);