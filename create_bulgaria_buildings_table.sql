-- DDL за създаване на таблицата bulgaria_buildings и нейния индекс.
-- Изпълнете това преди да изпълните Python програмата, която налива данни
-- Изтриване на таблицата, ако съществува (по избор, развийте ако е необходимо за чисти изпълнения)
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
