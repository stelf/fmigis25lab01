-- DDL for creating the bulgaria_buildings table and its index.
-- Run this script before executing the Python import script.

-- Drop table if it exists (optional, uncomment if needed for clean runs)
-- DROP TABLE IF EXISTS bulgaria_buildings;

CREATE TABLE IF NOT EXISTS bulgaria_buildings (
    id SERIAL PRIMARY KEY,
    confidence FLOAT,
    geom GEOMETRY(Polygon, 7801)
);

-- Create spatial index if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_class c
        JOIN   pg_namespace n ON n.oid = c.relnamespace
        WHERE  c.relname = 'bulgaria_buildings_geom_idx'
        AND    n.nspname = 'public' -- Adjust schema if needed
    ) THEN
        CREATE INDEX bulgaria_buildings_geom_idx ON bulgaria_buildings USING GIST (geom);
        RAISE NOTICE 'Index bulgaria_buildings_geom_idx created.';
    ELSE
        RAISE NOTICE 'Index bulgaria_buildings_geom_idx already exists.';
    END IF;
END;
$$;
