from pyspark.sql.functions import col, current_timestamp

def init_gold_layer(spark):
    """Ensures the Gold database exists."""
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    print("Database 'gold' is ready.")

def build_dim_customer(spark):
    print("Building gold.dim_customer...")
    
    # 1. Read Silver tables
    df_crm = spark.read.table("workspace.silver.crm_cust_info")
    df_erp_demo = spark.read.table("workspace.silver.erp_cust_az12")
    df_erp_loc = spark.read.table("workspace.silver.loc_a101")
    
    # 2. Join (Fixed cst_id = CID)
    df_customer = (df_crm
                 .join(df_erp_demo, df_crm.cst_key == df_erp_demo.CID, how="left")
                 .join(df_erp_loc, df_crm.cst_key == df_erp_loc.CID, how="left")
                 )
    
    # 3. Select clean columns (Dropping the redundant ERP 'CID' columns)
    df_final = df_customer.select(
        col("cst_id").alias("customer_id"),
        col("cst_firstname").alias("first_name"),
        col("cst_lastname").alias("last_name"),
        col("cst_marital_status").alias("marital_status"),
        col("cst_gndr").alias("crm_gender"), 
        col("GEN").alias("erp_gender"),
        col("BDATE").alias("birth_date"),
        col("CNTRY").alias("country"),
        current_timestamp().alias("_gold_timestamp")
    )
    
    # 4. Write
    df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_customer")
    print("dim_customer created successfully!")

def build_dim_product(spark):
    print("Building gold.dim_product...")
    
    df_prd = spark.read.table("workspace.silver.prd_info")
    df_cat = spark.read.table("workspace.silver.erp_px_cat_g1v2")
    
    # Join (Fixed prd_line = ID)
    df_product = df_prd.join(df_cat, df_prd.cat_id == df_cat.ID, how="left")
    
    df_final = df_product.select(
        col("prd_key").alias("product_key"),
        col("prd_nm").alias("product_name"),
        col("prd_cost").alias("cost"),
        col("prd_line").alias("line"),
        col("cat_id").alias("category_id"),
        col("CAT").alias("category_name"),
        col("SUBCAT").alias("subcategory_name"),
        current_timestamp().alias("_gold_timestamp")
    )
    
    df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_product")
    print("dim_product created successfully!")

def build_fact_sales(spark):
    print("Building gold.fact_sales...")
    
    df_sales = spark.read.table("workspace.silver.sales_details")
    df_prd = spark.read.table("workspace.silver.prd_info")
    df_crm = spark.read.table("workspace.silver.crm_cust_info")
    
    # Joins (Fixed sls_prd_key = prd_key)
    df_fact = (df_sales
               .join(df_prd, df_sales.sls_prd_key == df_prd.prd_id, how="left")
               .join(df_crm, df_sales.sls_cust_id == df_crm.cst_id, how="left")
               )
    
    df_final = df_fact.select(
        col("sls_ord_num").alias("order_number"),
        col("sls_prd_key").alias("product_key"),
        col("sls_cust_id").alias("customer_id"),
        col("sls_order_dt").alias("order_date"),
        col("sls_sales").alias("sales_amount"),
        col("sls_quantity").alias("quantity"),
        col("sls_price").alias("price"),
        current_timestamp().alias("_gold_timestamp")
    )
    
    df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_sales")
    print("fact_sales created successfully!")

if __name__ == "__main__":
    init_gold_layer(spark)
    build_dim_customer(spark)
    build_dim_product(spark)
    build_fact_sales(spark)
    print("Gold Layer processing complete!")