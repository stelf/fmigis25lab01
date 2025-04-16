# lab01/import_bulgaria_buildings.py

import sys
if not (sys.version_info.major == 3 and sys.version_info.minor == 11):
    print("This script requires Python 3.11.x. Exiting.")
    sys.exit(1)

import os
import json
import psycopg2
import gzip
import glob
import time
from dotenv import load_dotenv
from shapely.geometry.base import BaseGeometry
from shapely.geometry import shape
from shapely.errors import ShapelyError
from shapely import wkt

# --- Load environment variables for DB connection ---
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

input_dir_path = os.path.join(os.path.dirname(__file__), 'IN')
source_boundary_table = "adm_rayoni"
target_table = "bulgaria_buildings"

# --- Find all .gz files in the input directory ---
pattern = os.path.join(input_dir_path, '*.gz')
gz_file_paths = glob.glob(pattern)
if not gz_file_paths:
    print(f"No .gz files found in {input_dir_path}. Exiting.")
    exit()
print(f"Found {len(gz_file_paths)} .gz files in {input_dir_path}.")

# --- Connect to the database ---
def connect_db() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),   password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),   port    =os.getenv("PGPORT", "5432")
    )

# --- Calculate boundary geometry ---
def calculate_boundary(conn: psycopg2.extensions.connection, 
                       source_boundary_table: str) -> BaseGeometry | None:
    try:
        cursor = conn.cursor()
        boundary_sql = (
            f"""
            SELECT ST_AsText(
                st_simplify(
                    ST_Union(ST_Buffer(geom, 0.1)),
                    100
                )
            )
            FROM {source_boundary_table};
            """
        )
        cursor.execute(boundary_sql)
        result = cursor.fetchone()
        cursor.close()
        if result and result[0]:
            geom = wkt.loads(result[0])
            geom.srid = 7801 # Set SRID due to WKT not having it, and EWKT not supported
            return geom
        print(f"ERROR: Could not calculate boundary geometry from '{source_boundary_table}'.")
        return None
    except Exception as e:
        print(f"ERROR in calculate_boundary: {e}")
        return None

# --- Main spatial import logic ---
#    For each GeoJSONL feature:
#    - Decodes and parses the line
#    - Converts to Shapely geometry
#    - Checks intersection with the boundary (using Shapely)
#    - Inserts into DB if not a duplicate (using ST_Equals in SQL)
#
def process_and_insert_data(
                            data_iterator,
                            conn: psycopg2.extensions.connection,
                            boundary_geom: BaseGeometry ) -> int:
    cursor: psycopg2.extensions.cursor | None = None
    processed_count = inserted_count = skipped_count = 0
    commit_batch_size = 1000
    try:
        cursor = conn.cursor()
        print("Starting spatial import with intersection and deduplication checks...")
        for line_bytes in data_iterator:
            processed_count += 1
            if not line_bytes:
                continue
            try:
                line = line_bytes.decode('utf-8').strip()
                if not line:
                    continue
                feature = json.loads(line)
                geometry = feature.get('geometry')
                if geometry and geometry.get('type') == 'Polygon':
                    try:
                        feature_geom = shape(geometry)
                        # --- Shapely intersection: fast bbox check, then geometry check ---
                        if feature_geom.intersects(boundary_geom):
                            geom_wkt = feature_geom.wkt
                            # --- Insert only if not already present ---
                            cursor.execute(
                                """
                                INSERT INTO bulgaria_buildings (geom)
                                SELECT ST_GeomFromText(%s, 7801)
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM bulgaria_buildings
                                    WHERE ST_Equals(geom, ST_GeomFromText(%s, 7801))
                                );
                                """, (geom_wkt, geom_wkt)
                            )
                            if cursor.rowcount:
                                inserted_count += 1
                        else:
                            skipped_count += 1
                    except (ShapelyError, ValueError, TypeError) as shape_err:
                        print(f"Skipping line due to Shapely error: {shape_err} - Line: {line[:100]}...")
                        continue
                    # --- Commit in batches for performance ---
                    db_calls = processed_count - skipped_count
                    if db_calls > 0 and db_calls % commit_batch_size == 0:
                        conn.commit()
                        print(f"Processed {processed_count} lines ({skipped_count} skipped), committed. Inserted so far: {inserted_count}")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Skipping line: {line[:100]}... Error: {e}")
        conn.commit()
        print(f"Batch done. Processed: {processed_count}, Skipped: {skipped_count}, Inserted: {inserted_count}")
        return inserted_count
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn: conn.rollback()
        return 0
    except Exception as e:
        print(f"Unexpected error: {e}")
        if conn: conn.rollback()
        return 0
    finally:
        if cursor: cursor.close()

# --- Main entry point ---
def main() -> None:
    conn: psycopg2.extensions.connection | None = None
    total_inserted = 0
    boundary_geom = None
    total_start = time.time()
    
    try:
        # --- get connection
        conn = connect_db()
        print("Database connection successful.")
        conn.autocommit = False

        # --- Calculate boundary geometry as a separate step ---
        boundary_geom = calculate_boundary(conn, source_boundary_table)
        if not boundary_geom:
            if conn: conn.close()
            return

        # --- Process each .gz file ---
        for i, file_path in enumerate(gz_file_paths):
            file_name = os.path.basename(file_path)
            print(f"\n--- Processing file {i+1}/{len(gz_file_paths)}: {file_name} ---")
            t1 = time.time()
            try:
                with gzip.open(file_path, 'rb') as f_in:
                    inserted = process_and_insert_data(f_in, conn, boundary_geom)
                    total_inserted += inserted if inserted else 0
                print(f"--- Finished {file_name} in {time.time() - t1:.2f} s. Inserted: {inserted or 0} ---")
            except FileNotFoundError:
                print(f"ERROR: File not found: {file_path}. Skipping.")
                continue
            except gzip.BadGzipFile:
                print(f"ERROR: Bad Gzip file: {file_path}. Skipping.")
                if conn: conn.rollback()
                continue
            
        print(f"\n--- Import finished. Total features inserted: {total_inserted} ---")
        print(f"--- Total execution time: {time.time() - total_start:.2f} s ---")
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not establish initial database connection: {e}")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
