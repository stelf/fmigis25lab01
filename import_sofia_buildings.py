# lab01/import_bulgaria_buildings.py

import os
import json
import psycopg2
import gzip
import glob
import time
from dotenv import load_dotenv
from shapely.geometry import shape, box
from shapely.errors import ShapelyError
from shapely import wkb

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)
# Environment variables loaded: PGDATABASE, PGUSER, PGPASSWORD, PGHOST, PGPORT (optional, defaults to 5432)
# These variables are used directly in the code below.

input_dir_path = os.path.join(os.path.dirname(__file__), 'IN')
source_boundary_table = "adm_rayoni"
target_table = "validated_bulgaria_buildings"

def process_and_insert_data(data_iterator, conn, boundary_geom_wkb, boundary_box_shapely):
    """Processes GeoJSONL data, performs Shapely bbox check, calls PL/pgSQL function, returns count."""
    cursor = None
    processed_count = 0
    inserted_count_in_run = 0
    skipped_bbox_count = 0
    commit_batch_size = 1000

    try:
        cursor = conn.cursor()
        call_sql = f"SELECT process_geojson_line(%s, ST_GeomFromEWKB(%s));"
        print("Starting data processing with Shapely bounding box pre-check...")

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

                # Basic check for Polygon geometry
                if geometry and geometry.get('type') == 'Polygon':
                    try:
                        input_shape = shape(geometry)
                        feature_box = box(*input_shape.bounds)

                        # BBOX Intersection Check using Shapely
                        if feature_box.intersects(boundary_box_shapely):
                            geom_json = json.dumps(geometry)
                            cursor.execute(call_sql, (geom_json, boundary_geom_wkb))
                            result = cursor.fetchone()
                            status = result[0] if result else 0  # Default to 0 if no result

                            if status == 1:
                                inserted_count_in_run += 1
                        else:       # Skip DB call if bounding boxes don't intersect                            
                            skipped_bbox_count += 1

                    except (ShapelyError, ValueError, TypeError) as shape_err:
                        print(f"Skipping line due to Shapely error: {shape_err} - Line: {line[:100]}...")
                        continue  # Skip to next line

                    # Commit periodically
                    db_calls_made = processed_count - skipped_bbox_count
                    if db_calls_made > 0 and db_calls_made % commit_batch_size == 0:
                        conn.commit()
                        print(f"Processed {processed_count} lines ({skipped_bbox_count} skipped by bbox), committed transaction. Inserted so far: {inserted_count_in_run}")

            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {line[:100]}... Error: {e}")
            except UnicodeDecodeError as e:
                print(f"Skipping line due to decoding error: {e}")

        # Final commit
        conn.commit()
        print(f"Data processing for this batch completed. Total lines processed: {processed_count}. Skipped by BBOX check: {skipped_bbox_count}. Features inserted by function: {inserted_count_in_run}")
        return inserted_count_in_run

    except psycopg2.Error as e:
        print(f"Database error during processing: {e}")
        if conn:
            conn.rollback()
        return 0
    except Exception as e:
        print(f"An unexpected error occurred during data processing: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if cursor:
            cursor.close()

def find_gz_files(directory_path):
    """Finds all .gz files in the specified directory."""
    search_pattern = os.path.join(directory_path, '*.gz')
    gz_files = glob.glob(search_pattern)
    print(f"Found {len(gz_files)} .gz files in {directory_path}.")
    return gz_files

def main():
    gz_file_paths = find_gz_files(input_dir_path)
    if not gz_file_paths:
        print(f"No .gz files found in {input_dir_path}. Exiting.")
        return

    conn = None
    total_features_inserted = 0
    boundary_geom_wkb = None
    boundary_box_shapely = None
    total_start_time = time.time()

    try:
        # Use os.getenv directly in the connection string
        print(f"Connecting to database: dbname='{os.getenv('PGDATABASE')}' host='{os.getenv('PGHOST')}'")
        conn = psycopg2.connect(
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            host=os.getenv("PGHOST"),
            port=os.getenv("PGPORT", "5432")  # Default port if not set
        )
        print("Database connection successful.")
        conn.autocommit = False

        print(f"Calculating boundary geometry from '{source_boundary_table}'...")
        boundary_start_time = time.time()
        cursor = conn.cursor()

        # Calculate simplified union WKB
        boundary_sql = f"SELECT ST_AsEWKB(st_simplify(ST_Union(ST_Buffer(geom, 0.1)), 100)) FROM {source_boundary_table};"
        cursor.execute(boundary_sql)
        result = cursor.fetchone()
        if result and result[0]:
            boundary_geom_wkb = result[0]
            boundary_end_time = time.time()
            print(f"Boundary geometry WKB calculated successfully in {boundary_end_time - boundary_start_time:.2f} seconds.")

            # Calculate the bounding box using Shapely
            print("Calculating boundary bounding box using Shapely...")
            boundary_geom_shapely = wkb.loads(boundary_geom_wkb)
            boundary_box_shapely = box(*boundary_geom_shapely.bounds)
            print(f"Boundary bounding box calculated: {boundary_box_shapely.bounds}")

        else:
            print(f"ERROR: Could not calculate boundary geometry WKB from '{source_boundary_table}'. Ensure it exists and contains valid geometries.")
            cursor.close()
            if conn:
                conn.close()
            return

        cursor.close()

        for i, file_path in enumerate(gz_file_paths):
            file_name = os.path.basename(file_path)
            print(f"\n--- Processing file {i+1}/{len(gz_file_paths)}: {file_name} ---")
            file_start_time = time.time()

            try:
                with gzip.open(file_path, 'rb') as f_in:
                    features_in_run = process_and_insert_data(f_in, conn, boundary_geom_wkb, boundary_box_shapely)
                    total_features_inserted += features_in_run if features_in_run else 0

                file_end_time = time.time()
                print(f"--- Finished processing {file_name} in {file_end_time - file_start_time:.2f} seconds. Inserted: {features_in_run or 0} ---")

            except FileNotFoundError:
                print(f"ERROR: File not found: {file_path}. Skipping.")
                continue
            except gzip.BadGzipFile:
                print(f"ERROR: Bad Gzip file: {file_path}. Skipping.")
                if conn:
                    conn.rollback()
                continue

        total_end_time = time.time()
        print(f"\n--- Import finished. Total features inserted: {total_features_inserted} ---")
        print(f"--- Total execution time: {total_end_time - total_start_time:.2f} seconds ---")

    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not establish initial database connection: {e}")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
