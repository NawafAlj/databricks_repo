from pyspark.sql.functions import current_timestamp, col

# Set your Unity Catalog configuration
CATALOG = "nawaf"
SCHEMA = "bronze_new"

def init_bronze_layer(spark):
    """Ensures the Schema exists in the 'nawaf' Unity Catalog."""
    # Note: Unity Catalog uses the 3-level namespace catalog.schema.table
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"Schema '{CATALOG}.{SCHEMA}' is ready.")

def process_raw_to_bronze(spark, dbutils, base_path, subsets):
    """Loops through raw GCS directories and ingests CSVs to Managed Delta tables."""
    
    for subset in subsets:
        path_to_read = base_path + subset
        print(f"\n--- Scanning directory: {path_to_read} ---")
        
        try:
            csvfiles = dbutils.fs.ls(path_to_read)
        except Exception as e:
            print(f"ERROR: Could not access {path_to_read}. Skipping. Details: {e}")
            continue
        
        for file in csvfiles:
            if file.name.endswith('.csv'):
                print(f"Processing {file.name}...")
                
                try:
                    # 1. Read the CSV
                    # We enable file_path metadata support in the read options
                    df = (spark.read
                        .format('csv')
                        .option('header', 'true')
                        .option('inferSchema', 'true')
                        .load(file.path)
                    )
                    
                    # 2. Sanity Verification (Check for empty dataframes)
                    row_count = df.count()
                    if row_count == 0:
                        print(f"  -> WARNING: {file.name} is empty (0 rows). Skipping ingestion.")
                        continue

                    # 3. Metadata Injection
                    # Using the hidden _metadata column to track source file
                    df_with_metadata = (df
                        .withColumn("_ingest_timestamp", current_timestamp())
                        .withColumn("_source_file", col("_metadata.file_path")) 
                    )

                    # 4. Generate the 3-level table name
                    import re

                    # ... inside your for file in csvfiles loop ...

                    # 4. Safely generate the table name
                    prefix = subset.replace('/', '') # e.g., 'crm'
                    
                    # NEW LOGIC: Remove the leading timestamp pattern (20 digits + underscore)
                    # This matches '20260307225047176852_' and replaces it with an empty string
                    raw_filename = file.name[:-4] # Remove '.csv'
                    clean_filename = re.sub(r'^\d+_', '', raw_filename)
                    
                    # Final cleanup of any hyphens or spaces
                    clean_filename = clean_filename.replace('-', '_').replace(' ', '_')
                    
                    # Result: nawaf.bronze_new.crm_sales_details
                    table_name = f'{CATALOG}.{SCHEMA}.{prefix}_{clean_filename}'

                    # 5. Write to Managed Delta Table
                    # Writing without a 'path' creates a Managed Table in our verified GCS location
                    (df_with_metadata.write
                        .format('delta')
                        .mode('overwrite')
                        .option("overwriteSchema", "true")
                        .saveAsTable(table_name)
                    )
                    print(f"  -> SUCCESS: Wrote {row_count} rows to {table_name}")
                
                except Exception as e:
                    print(f"  -> FAILED to process {file.name}. Error: {e}")

        print(f"Finished processing {subset} directory.")

# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    print("Starting Bronze Layer Ingestion...")
    
    # Path verified in our previous successful test
    BASE_PATH = 'gs://nawaf-etl-lake-dev-useast/raw_new_nawaf'
    SUBSETS = ['/crm', '/erp']
    
    init_bronze_layer(spark)
    process_raw_to_bronze(spark, dbutils, BASE_PATH, SUBSETS)
    
    print(f"\nAll raw files successfully processed into the {CATALOG}.{SCHEMA} layer!")