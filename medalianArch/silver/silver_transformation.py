from pyspark.sql.functions import (
    col, trim, lower, upper, substring, initcap, 
    to_date, try_to_date, current_timestamp, when, regexp_replace, length
)

# Optional: If running outside a notebook, you might need to import SparkSession
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()

def init_silver_layer(spark):
    """Ensures the Silver database exists before processing."""
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

def process_crm_cust_info(spark):
    source = 'workspace.bronze_new.crm_20260221135014660445_cust_info'
    target = 'workspace.silver.crm_cust_info'
    
    df = spark.read.table(source)
    df_silver = (df
        .dropna(subset=['cst_id', 'cst_key'])
        .dropDuplicates(['cst_key'])
        .withColumn('cst_firstname', initcap(trim(col('cst_firstname'))))
        .withColumn('cst_lastname', initcap(trim(col('cst_lastname'))))
        .withColumn('cst_gndr', 
                    when(col('cst_gndr').isNotNull(), upper(substring(trim(col('cst_gndr')), 1, 1)))
                    .otherwise('Unknown'))
        .withColumn('cst_marital_status', 
                    when(lower(trim(col('cst_marital_status'))).startswith('s'), 'Single')
                    .when(lower(trim(col('cst_marital_status'))).startswith('m'), 'Married')
                    .otherwise('Unknown'))
        .withColumn('cst_create_date', to_date(col('cst_create_date'), 'MM/dd/yyyy'))
        .withColumn("_cleaned_timestamp", current_timestamp())
    )
    
    df_silver.write.format('delta').mode('overwrite').saveAsTable(target)
    print(f"Successfully cleaned cust_info and wrote to {target}")

def process_crm_prd_info(spark):
    source = 'workspace.bronze_new.crm_20260221135017183310_prd_info'
    target = 'workspace.silver.prd_info'
    
    df = spark.read.table(source)
    df_silver = (df
        .dropna(subset=['prd_id', 'prd_key'])
        .dropDuplicates(['prd_key'])
        .withColumn('prd_nm', initcap(trim(col('prd_nm'))))
        .withColumn('prd_line', upper(trim(col('prd_line'))))
        .withColumn('prd_cost', col('prd_cost').cast('decimal(10,2)'))
        .withColumn("_cleaned_timestamp", current_timestamp())
        .withColumn("cat_id",substring(col("prd_key"),1, 5))
        .withColumn("prd_id",substring(col("prd_key"),7,length(col("prd_key"))))
    )
    
    # Add the overwriteSchema option before saving
    df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable(target)
    print(f"Successfully cleaned prd_info and wrote to {target}")

def process_sales_details(spark):
    source = 'workspace.bronze_new.crm_20260221135017680009_sales_details'
    target = 'workspace.silver.sales_details'
    
    df = spark.read.table(source)
    df_silver = (df
        .dropna(subset=['sls_ord_num', 'sls_prd_key', 'sls_cust_id'])
        .dropDuplicates(['sls_ord_num', 'sls_prd_key'])
        .withColumn('sls_order_dt', try_to_date(col('sls_order_dt').cast('string'), 'yyyyMMdd'))
        .withColumn('sls_ship_dt', try_to_date(col('sls_ship_dt').cast('string'), 'yyyyMMdd'))
        .withColumn('sls_due_dt', try_to_date(col('sls_due_dt').cast('string'), 'yyyyMMdd'))
        .withColumn('sls_sales', regexp_replace(col('sls_sales').cast('string'), r'[\$,]', '').cast('decimal(10,2)'))
        .withColumn('sls_price', regexp_replace(col('sls_price').cast('string'), r'[\$,]', '').cast('decimal(10,2)'))
        .withColumn('sls_quantity', col('sls_quantity').cast('int'))
        .withColumn("_cleaned_timestamp", current_timestamp())
    )
    
# Add the overwriteSchema option here as well
    df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable(target)
    print(f"Successfully cleaned sales_details and wrote to {target}")

def process_erp_cust_az12(spark):
    source = 'workspace.bronze_new.erp_20260221135020384837_cust_az12'
    target = 'workspace.silver.erp_cust_az12'
    
    df = spark.read.table(source)
    df_silver = (df
        .dropna(subset=['CID'])
        .dropDuplicates(['CID'])
        .withColumn('GEN', upper(substring(trim(col('GEN')), 1, 1)))
        .withColumn('BDATE', try_to_date(col('BDATE').cast('string'), 'yyyy-MM-dd'))
        .withColumn("_cleaned_timestamp", current_timestamp())
        .withColumn("CID", substring(col("CID"), 4, length(col("CID"))))
    )
    
    df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable(target)
    print(f"Successfully cleaned cust_az12 and wrote to {target}")

def process_erp_loc_a101(spark):
    source = 'workspace.bronze_new.erp_20260221135021821657_loc_a101'
    target = 'workspace.silver.loc_a101'
    
    df = spark.read.table(source)
    df_silver = (df
        .dropna(subset=['CID'])
        .dropDuplicates(['CID'])
        .withColumn('CNTRY', upper(trim(col('CNTRY'))))
        .fillna({'CNTRY': 'UNKNOWN'})
        .withColumn("_cleaned_timestamp", current_timestamp())
        .withColumn("CID",regexp_replace(col("CID"),"-",""))
    )
    
    df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable(target)
    print(f"Successfully cleaned loc_a101 and wrote to {target}")

def process_erp_px_cat(spark):
    source = 'workspace.bronze_new.erp_20260221135023079340_px_cat_g1v2'
    target = 'workspace.silver.erp_px_cat_g1v2'
    
    df = spark.read.table(source)
    df_silver = (df
        .dropna(subset=['ID'])
        .dropDuplicates(['ID'])
        .withColumn('CAT', upper(trim(col('CAT'))))
        .withColumn('SUBCAT', upper(trim(col('SUBCAT'))))
        .withColumn('MAINTENANCE', upper(trim(col('MAINTENANCE'))))
        .withColumn('MAINTENANCE', 
                    when(col('MAINTENANCE').isin('Y', '1', 'TRUE', 'YES', 'REQ'), 'YES')
                    .when(col('MAINTENANCE').isin('N', '0', 'FALSE', 'NO', 'NONE'), 'NO')
                    .otherwise(col('MAINTENANCE')))
        .fillna({'MAINTENANCE': 'UNKNOWN'})
        .withColumn("_cleaned_timestamp", current_timestamp())
        .withColumn("ID",regexp_replace(col("ID"),"_", "-"))
    )
    
    df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable(target)
    print(f"Successfully cleaned px_cat_g1v2 and wrote to {target}")

# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    print("Starting Silver Layer Processing...")
    
    init_silver_layer(spark)
    
    process_crm_cust_info(spark)
    process_crm_prd_info(spark)
    process_sales_details(spark)
    process_erp_cust_az12(spark)
    process_erp_loc_a101(spark)
    process_erp_px_cat(spark)
    
    print("All Silver Layer tables processed successfully!")