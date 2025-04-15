-- Assumes the target table exists:
-- CREATE TABLE IF NOT EXISTS validated_bulgaria_buildings (
--     id SERIAL PRIMARY KEY,
--     geom GEOMETRY(Geometry, 4326) -- Assuming WGS84
-- );

CREATE OR REPLACE FUNCTION process_geojson_line(geojson_line TEXT, boundary_geom GEOMETRY)
RETURNS INTEGER AS $$
DECLARE
    new_geom GEOMETRY;
    return_status INTEGER := 0; -- 0 = not inserted, 1 = inserted, -1 = error
BEGIN
    IF trim(geojson_line) = '' THEN
        RETURN 0; -- Not inserted (empty line)
    END IF;

    BEGIN
        -- 2. Convert GeoJSON string to Geometry (assuming WGS84)
        new_geom := st_transform(ST_GeomFromGeoJSON(geojson_line), 7801);

        RAISE INFO 'new_geom %', ST_AsText(new_geom);
        -- 3. Validate and check if within the boundary
        IF new_geom IS NOT NULL AND ST_IsValid(new_geom) THEN
            IF ST_Within(new_geom, boundary_geom) THEN
                -- 4. Insert into target table if valid and within boundary
                INSERT INTO bulgaria_buildings (geom) VALUES (new_geom);
                return_status := 1; -- Inserted
            ELSE
                -- Geometry is valid but outside boundary
                return_status := 0; -- Not inserted
            END IF;
        ELSE
             -- Invalid GeoJSON or resulting geometry is invalid
             RAISE WARNING 'Invalid GeoJSON or resulting geometry is invalid. Line: %', left(geojson_line, 100);
             return_status := 0; -- Not inserted
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error processing GeoJSON line. SQLSTATE: %, SQLERRM: %. Line: %', SQLSTATE, SQLERRM, left(geojson_line, 100);
            return_status := -1; -- Indicate error
    END;

    RETURN return_status;
END;
$$ LANGUAGE plpgsql;

-- Example Usage (within SQL or from Python):
-- -- -- 

-- SELECT process_geojson_line('{"type": "Polygon","coordinates": [[[22.393597777340727, 43.79349324638614], [22.393579825695436, 43.79351841928142], [22.393535298402863, 43.79350187386734], [22.39361497689199, 43.79339014370331], [22.393774002937, 43.793449234557286], [22.393712279354276, 43.79353578716055], [22.393597777340727, 43.79349324638614]]]}', 
--  (select ST_Union(ST_Buffer(geom, 0.1)) FROM adm_rayoni)
-- ); 


-- SELECT process_geojson_line(
--     '{"type": "Polygon", "coordinates": [[[23.06283090234993, 42.717297939044116], [23.062886019308884, 42.71728030184921], [23.062899768333804, 42.717303491256274], [23.062844649518052, 42.717321129038766], [23.06283090234993, 42.717297939044116]]]}', 
--     (select ST_Union(ST_Buffer(geom, 0.1)) FROM adm_rayoni)
-- ); 





-- Example valid point within BG
-- SELECT process_geojson_line('{"type": "Point", "coordinates": [10.0, 42.0]}'); -- Example point outside BG
-- SELECT process_geojson_line('invalid json'); -- Example error case