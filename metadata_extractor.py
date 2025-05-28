import mysql.connector
import json
import decimal
import datetime # Added missing import

# --- Database Connection Details (same as database_setup.py) ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',      # Your MySQL username
    'password': '', # Your MySQL password
    'database': 'semantic_catalog_db'
}

SAMPLE_DATA_LIMIT = 5  # Number of sample rows to fetch

def extract_metadata():
    """Connects to the database and extracts schema metadata and sample data."""
    conn = None
    metadata = {
        "database_name": DB_CONFIG['database'],
        "tables": {}
    }

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True) # Use dictionary cursor for easier row access
        db_name = DB_CONFIG['database']

        # 1. List Tables
        cursor.execute("SHOW TABLES")
        tables = [row[f'Tables_in_{db_name}'] for row in cursor.fetchall()]

        for table_name in tables:
            print(f"Extracting metadata for table: {table_name}...")
            table_info = {
                "columns": [],
                "primary_keys": [],
                "foreign_keys": [],
                "sample_data": []
            }

            # 2. Get Columns and Primary Keys
            # Using information_schema.columns for more detail
            sql_columns = f"""
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    COLUMN_TYPE, 
                    IS_NULLABLE, 
                    COLUMN_KEY, 
                    EXTRA 
                FROM 
                    information_schema.columns 
                WHERE 
                    TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION;
            """
            cursor.execute(sql_columns, (db_name, table_name))
            for col_row in cursor.fetchall():
                column_details = {
                    "name": col_row['COLUMN_NAME'],
                    "data_type": col_row['DATA_TYPE'],
                    "column_type": col_row['COLUMN_TYPE'], # e.g., varchar(255), int(11)
                    "is_nullable": col_row['IS_NULLABLE'] == 'YES',
                    "is_primary_key": col_row['COLUMN_KEY'] == 'PRI',
                    "extra": col_row['EXTRA']
                }
                table_info["columns"].append(column_details)
                if column_details["is_primary_key"]:
                    table_info["primary_keys"].append(col_row['COLUMN_NAME'])
            
            # 3. Get Foreign Keys
            sql_fks = f"""
                SELECT 
                    CONSTRAINT_NAME, 
                    COLUMN_NAME, 
                    REFERENCED_TABLE_NAME, 
                    REFERENCED_COLUMN_NAME 
                FROM 
                    information_schema.KEY_COLUMN_USAGE 
                WHERE 
                    TABLE_SCHEMA = %s 
                    AND TABLE_NAME = %s 
                    AND REFERENCED_TABLE_NAME IS NOT NULL;
            """
            cursor.execute(sql_fks, (db_name, table_name))
            for fk_row in cursor.fetchall():
                table_info["foreign_keys"].append({
                    "constraint_name": fk_row['CONSTRAINT_NAME'],
                    "column_name": fk_row['COLUMN_NAME'],
                    "references_table": fk_row['REFERENCED_TABLE_NAME'],
                    "references_column": fk_row['REFERENCED_COLUMN_NAME']
                })

            # 4. Fetch Sample Data
            # Be cautious with large text/blob fields in sample data for LLM context
            # For simplicity, selecting all columns here.
            if table_info["columns"]: # Only fetch if columns exist
                try:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT {SAMPLE_DATA_LIMIT}")
                    sample_rows = cursor.fetchall()
                    # Convert datetime/date objects and Decimal objects to string for JSON serialization
                    for row in sample_rows:
                        for key, value in row.items():
                            if hasattr(value, 'isoformat'): # Check for date/datetime objects
                                row[key] = value.isoformat()
                            elif isinstance(value, decimal.Decimal): # Check for Decimal objects
                                row[key] = str(value) # Convert Decimal to string
                    table_info["sample_data"] = sample_rows
                except mysql.connector.Error as sample_err:
                    print(f"Could not fetch sample data for {table_name}: {sample_err}")
                    table_info["sample_data"] = [{"error": str(sample_err)}]
            
            metadata["tables"][table_name] = table_info

        return metadata

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed.")

def custom_json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, decimal.Decimal):
        return str(obj)  # Convert Decimal to string
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat() # Convert date/datetime to ISO 8601 string
    if isinstance(obj, bytes):
        try:
            return obj.decode('utf-8') # Try to decode bytes to string
        except UnicodeDecodeError:
            return obj.hex() # If not decodable, return hex representation
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

if __name__ == "__main__":
    print("Starting metadata extraction...")
    extracted_data = extract_metadata()

    if extracted_data:
        print("\n--- Extracted Metadata (JSON) ---")
        # Pretty print the JSON
        print(json.dumps(extracted_data, indent=4, default=custom_json_serializer))
        
        # Optionally, save to a file
        try:
            with open("extracted_metadata.json", "w") as f:
                json.dump(extracted_data, f, indent=4, default=custom_json_serializer)
            print("\nSuccessfully saved metadata to extracted_metadata.json")
        except IOError as e:
            print(f"\nError saving metadata to file: {e}")
        except Exception as e:
            print(f"\nAn unexpected error occurred while saving to file: {e}")
    else:
        print("Metadata extraction failed.")

    print("\nMetadata extraction script finished.")
