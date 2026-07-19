CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE demo.fresh (id serial primary key, updated_at timestamptz DEFAULT now());
CREATE TABLE demo.stale (id serial primary key, updated_at timestamptz DEFAULT now());
-- seed both once; a sidecar keeps inserting into demo.fresh, demo.stale is left to rot
INSERT INTO demo.fresh (updated_at) VALUES (now());
INSERT INTO demo.stale (updated_at) VALUES (now());
