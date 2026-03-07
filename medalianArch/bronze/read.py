from pyspark.sql.functions import current_timestamp, col

def init_bronze_layer(spark):
    """Ensures the Bronze database exists before processing."""
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze_new")
    print("Database 'bronze_new' is ready.")

def process_raw_to_bronze(spark, dbutils, base_path, subsets):
    """Loops through raw GCS directories and ingests CSVs to Bronze Delta tables."""
    
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
                    df_with_metadata = (df
                        .withColumn("_ingest_timestamp", current_timestamp())
                        .withColumn("_source_file", col("_metadata.file_path")) 
                    )

                    # 4. Safely generate the table name (Replacing hyphens with underscores)
                    prefix = subset.replace('/', '') 
                    clean_filename = file.name[:-4].replace('-', '_').replace(' ', '_')
                    table_name = f'bronze_new.{prefix}_{clean_filename}'

                    # 5. Write to Delta
                    (df_with_metadata.write
                        .format('delta')
                        .mode('overwrite')
                        .saveAsTable(table_name)
                    )
                    print(f"  -> SUCCESS: Wrote {row_count} rows to {table_name}")
                
                except Exception as e:
                    # If this specific file fails, catch the error and keep the loop running!
                    print(f"  -> FAILED to process {file.name}. Error: {e}")

        print(f"Finished processing {subset} directory.")

# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    print("Starting Bronze Layer Ingestion...")
    
    BASE_PATH = 'gs://nawaf-etl-lake-dev-useast/raw'
    SUBSETS = ['/crm', '/erp']
    
    init_bronze_layer(spark)
    process_raw_to_bronze(spark, dbutils, BASE_PATH, SUBSETS)
    
    print("\nAll raw files successfully processed into the Bronze layer!")