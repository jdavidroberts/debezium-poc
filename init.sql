-- init.sql — Bootstrap the CDC source database
-- Runs automatically on first container start (mounted into /docker-entrypoint-initdb.d/).
--
-- wal_level=logical is set via the postgres command args in docker-compose.yml
-- (it's a server-level setting that cannot be changed per-database).

-- ══════════════════════════════════════════════════════════════════════
-- Schema
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE merchants (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payments (
    id           SERIAL PRIMARY KEY,
    merchant_id  INTEGER      NOT NULL REFERENCES merchants(id),
    amount       NUMERIC(10,2) NOT NULL,
    status       VARCHAR(50)  NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- REPLICA IDENTITY FULL gives Debezium the complete "before" image on
-- UPDATE and DELETE events (not just the PK).  Useful for auditing; the
-- default (PK-only) would also work for this POC's MERGE logic.
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE payments  REPLICA IDENTITY FULL;

-- ══════════════════════════════════════════════════════════════════════
-- Sample data (will appear in the Debezium initial snapshot as op='r')
-- ══════════════════════════════════════════════════════════════════════

INSERT INTO merchants (name) VALUES
    ('Acme Corp'),
    ('Globex Inc'),
    ('Umbrella LLC'),
    ('Stark Industries'),
    ('Wayne Enterprises');

INSERT INTO payments (merchant_id, amount, status) VALUES
    (1,  100.50, 'completed'),
    (1,  250.00, 'pending'),
    (2,   75.25, 'completed'),
    (3,  500.00, 'failed'),
    (4, 1200.00, 'completed'),
    (5,   89.99, 'pending'),
    (2,  340.00, 'completed'),
    (3,   62.50, 'pending');
