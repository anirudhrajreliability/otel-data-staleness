-- Integration-stack database seed.
-- Creates the tables the Collector's SQL/db_migration scrapers and the SDK
-- service read, plus tables used by the accuracy and chaos assertions.
CREATE SCHEMA IF NOT EXISTS demo;

-- fresh: a sidecar inserts every few seconds -> age stays ~0 (control).
CREATE TABLE demo.fresh (id serial primary key, updated_at timestamptz DEFAULT now());
INSERT INTO demo.fresh (updated_at) VALUES (now());

-- stale: seeded once, never touched -> age climbs and breaches the 60s SLA.
CREATE TABLE demo.stale (id serial primary key, updated_at timestamptz DEFAULT now());
INSERT INTO demo.stale (updated_at) VALUES (now());

-- accuracy: intentionally EMPTY at seed time. The accuracy step injects one row
-- with a KNOWN epoch (now - 120s) at test time, then asserts last_update.timestamp
-- equals that epoch and age ~= 120s. (SLA high so it doesn't muddy the breach test.)
CREATE TABLE demo.accuracy (id serial primary key, updated_at timestamptz);

-- skew: a FUTURE timestamp -> exercises clock-skew clamping (age must clamp >=0,
-- never go negative). last_update.timestamp still reports the future epoch.
CREATE TABLE demo.skew (id serial primary key, updated_at timestamptz);
INSERT INTO demo.skew (updated_at) VALUES (now() + interval '1 hour');

-- schema_migrations: db_migration drift source reads the latest applied version.
CREATE TABLE demo.schema_migrations (version text, applied_at timestamptz DEFAULT now());
INSERT INTO demo.schema_migrations (version) VALUES ('0005');

-- sdk_orders: read by the Python SDK service (SDK-in-the-live-path check).
CREATE TABLE demo.sdk_orders (id serial primary key, updated_at timestamptz DEFAULT now());
INSERT INTO demo.sdk_orders (updated_at) VALUES (now());
