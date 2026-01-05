from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round, datediff, to_date
import argparse
import sys

def process_car_rental_data(data_date):
    print(f"[INFO] Starting Car Rental Data Processing for date: {data_date}")
    print(f"[INFO] Python version: {sys.version}")
    
    # Initialize SparkSession with Snowflake-optimized configuration
    spark = SparkSession.builder \
        .appName("CarRentalDataProcessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.network.timeout", "900s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print(f"[INFO] SparkSession created successfully")
    print(f"[INFO] Spark version: {spark.version}")


    # Define GCS file path based on the execution date argument (yyyymmdd)
    # Read from the correct bucket (was missing "-bucket")
    gcs_file_path = f"gs://snowflake-projects-test-gds-bucket/car_rental_data/car_rental_daily_data/car_rental_{data_date}.json"
    print(f"[INFO] Reading JSON data from: {gcs_file_path}")

    # Read raw JSON data (multiline JSON array)
    try:
        raw_df = spark.read.option("multiline", "true").json(gcs_file_path)
        raw_count = raw_df.count()
        print(f"[INFO] Successfully read {raw_count} rows from JSON file")
        if raw_count == 0:
            print("[WARNING] No data found in JSON file! Exiting.")
            return
    except Exception as e:
        print(f"[ERROR] Failed to read JSON file: {str(e)}")
        raise

    # Data validation: filter out rows missing mandatory fields
    print("[INFO] Validating data - filtering rows with missing mandatory fields")
    validated_df = raw_df.filter(
        col("rental_id").isNotNull() & 
        col("customer_id").isNotNull() & 
        col("car.make").isNotNull() & 
        col("car.model").isNotNull() & 
        col("car.year").isNotNull() & 
        col("rental_period.start_date").isNotNull() & 
        col("rental_period.end_date").isNotNull() & 
        col("rental_location.pickup_location").isNotNull() & 
        col("rental_location.dropoff_location").isNotNull() & 
        col("amount").isNotNull() & 
        col("quantity").isNotNull()
    )
    validated_count = validated_df.count()
    print(f"[INFO] After validation: {validated_count} rows remain (filtered out {raw_count - validated_count} invalid rows)")

    # Transformation 1: Convert date strings to Date type and calculate rental duration in days
    print("[INFO] Applying transformations: date conversion, rental duration, amounts, and flags")
    transformed_df = validated_df.withColumn(
        "start_date_parsed", 
        to_date(col("rental_period.start_date"), "yyyy-MM-dd")
    ).withColumn(
        "end_date_parsed", 
        to_date(col("rental_period.end_date"), "yyyy-MM-dd")
    ).withColumn(
        "rental_duration_days", 
        datediff(col("end_date_parsed"), col("start_date_parsed"))
    )
    # Transformation 2: Derive additional quantitative attributes
    transformed_df = transformed_df.withColumn(
        "total_rental_amount", 
        col("amount") * col("quantity")
    ).withColumn(
        "average_daily_rental_amount", 
        when(col("rental_duration_days") > 0, 
             round(col("total_rental_amount") / col("rental_duration_days"), 2)
        ).otherwise(lit(0.0))
    ).withColumn(
        "is_long_rental", 
        when(col("rental_duration_days") > 7, lit(True)).otherwise(lit(False))
    )
    print(f"[INFO] Transformations completed")


    # Read dimension tables from Snowflake (via Spark connector)
    print("[INFO] Configuring Snowflake connection options")
    snowflake_options = {
        "sfURL": "lrbfxhz-gj86356.snowflakecomputing.com",  # Remove https:// protocol
        "sfAccount": "lrbfxhz-gj86356", 
        "sfUser": "iamabhaydawar",
        "sfPassword": "Smokeweed69420$",
        "sfDatabase": "car_rental",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN",
        # Add timeout configurations to handle network issues
        "sfTimeout": "900",  # 15 minutes
        "connect_timeout": "900",
        "network_timeout": "900"
    }

    # Source name alias for Snowflake Spark connector
    SNOWFLAKE_SOURCE_NAME = "snowflake"

    # Dimension loads with error handling and caching
    print("[INFO] Reading dimension tables from Snowflake...")
    try:
        print("[INFO] Reading car_dim...")
        car_dim_df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**snowflake_options) \
            .option("dbtable", "car_dim") \
            .load()
        car_dim_df.cache()
        car_count = car_dim_df.count()
        print(f"[INFO] car_dim loaded: {car_count} rows")

        print("[INFO] Reading location_dim...")
        location_dim_df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**snowflake_options) \
            .option("dbtable", "location_dim") \
            .load()
        location_dim_df.cache()
        loc_count = location_dim_df.count()
        print(f"[INFO] location_dim loaded: {loc_count} rows")

        print("[INFO] Reading date_dim...")
        date_dim_df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**snowflake_options) \
            .option("dbtable", "date_dim") \
            .load()
        date_dim_df.cache()
        date_count = date_dim_df.count()
        print(f"[INFO] date_dim loaded: {date_count} rows")

        # Filter customer_dim for current records only (SCD2)
        print("[INFO] Reading customer_dim (filtering for current records only)...")
        customer_dim_df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**snowflake_options) \
            .option("dbtable", "customer_dim") \
            .load() \
            .filter(col("is_current") == True)
        customer_dim_df.cache()
        cust_count = customer_dim_df.count()
        print(f"[INFO] customer_dim (current) loaded: {cust_count} rows")
        
        print("[INFO] All dimension tables loaded successfully!")
    except Exception as e:
        print(f"[ERROR] Failed to read dimension tables from Snowflake: {str(e)}")
        print(f"[ERROR] Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise
        
    # Join raw data with dimension tables to derive surrogate keys
    print("[INFO] Starting dimension joins...")
    # 1) car_dim
    print("[INFO] Joining with car_dim...")
    fact_df = transformed_df.alias("raw") \
        .join(car_dim_df.alias("car"), 
              (col("raw.car.make") == col("car.make")) & 
              (col("raw.car.model") == col("car.model")) & 
              (col("raw.car.year") == col("car.year")),
              "left"
        ) \
        .select(
            col("raw.rental_id"),
            col("raw.customer_id"),
            col("car.car_key"),
            col("raw.rental_location.pickup_location").alias("pickup_location"),
            col("raw.rental_location.dropoff_location").alias("dropoff_location"),
            col("raw.start_date_parsed").alias("start_date"),
            col("raw.end_date_parsed").alias("end_date"),
            col("raw.amount"),
            col("raw.quantity"),
            col("raw.rental_duration_days"),
            col("raw.total_rental_amount"),
            col("raw.average_daily_rental_amount"),
            col("raw.is_long_rental")
        )
    fact_count = fact_df.count()
    print(f"[INFO] After car_dim join: {fact_count} rows")

    # 2) location_dim for pickup_location
    print("[INFO] Joining with location_dim (pickup)...")
    fact_df = fact_df.alias("fact") \
        .join(location_dim_df.alias("pickup_loc"), col("fact.pickup_location") == col("pickup_loc.location_name"), "left") \
        .withColumnRenamed("location_key", "pickup_location_key") \
        .drop("pickup_location")

    # 3) location_dim for dropoff_location
    print("[INFO] Joining with location_dim (dropoff)...")
    fact_df = fact_df.alias("fact") \
        .join(location_dim_df.alias("dropoff_loc"), col("fact.dropoff_location") == col("dropoff_loc.location_name"), "left") \
        .withColumnRenamed("location_key", "dropoff_location_key") \
        .drop("dropoff_location")

    # 4) date_dim for start_date
    print("[INFO] Joining with date_dim (start_date)...")
    fact_df = fact_df.alias("fact") \
        .join(date_dim_df.alias("start_date_dim"), col("fact.start_date") == col("start_date_dim.date"), "left") \
        .withColumnRenamed("date_key", "start_date_key") \
        .drop("start_date")

    # 5) date_dim for end_date
    print("[INFO] Joining with date_dim (end_date)...")
    fact_df = fact_df.alias("fact") \
        .join(date_dim_df.alias("end_date_dim"), col("fact.end_date") == col("end_date_dim.date"), "left") \
        .withColumnRenamed("date_key", "end_date_key") \
        .drop("end_date")

    # 6) customer_dim to get customer_key (filtered for is_current=true)
    print("[INFO] Joining with customer_dim...")
    fact_df = fact_df.alias("fact") \
        .join(customer_dim_df.alias("cust"), col("fact.customer_id") == col("cust.customer_id"), "left") \
        .select(
            col("fact.rental_id"),
            col("cust.customer_key"),
            col("fact.car_key"),
            col("fact.pickup_location_key"),
            col("fact.dropoff_location_key"),
            col("fact.start_date_key"),
            col("fact.end_date_key"),
            col("fact.amount"),
            col("fact.quantity"),
            col("fact.rental_duration_days"),
            col("fact.total_rental_amount"),
            col("fact.average_daily_rental_amount"),
            col("fact.is_long_rental")
        )
    final_count = fact_df.count()
    print(f"[INFO] After all joins: {final_count} rows ready for fact table")
    
    # Data validation: Check for NULL foreign keys and log warnings
    print("[INFO] Validating foreign keys...")
    null_customer_key = fact_df.filter(col("customer_key").isNull()).count()
    null_car_key = fact_df.filter(col("car_key").isNull()).count()
    null_pickup_loc = fact_df.filter(col("pickup_location_key").isNull()).count()
    null_dropoff_loc = fact_df.filter(col("dropoff_location_key").isNull()).count()
    null_start_date = fact_df.filter(col("start_date_key").isNull()).count()
    null_end_date = fact_df.filter(col("end_date_key").isNull()).count()
    
    print(f"[WARNING] Rows with NULL customer_key: {null_customer_key}")
    print(f"[WARNING] Rows with NULL car_key: {null_car_key}")
    print(f"[WARNING] Rows with NULL pickup_location_key: {null_pickup_loc}")
    print(f"[WARNING] Rows with NULL dropoff_location_key: {null_dropoff_loc}")
    print(f"[WARNING] Rows with NULL start_date_key: {null_start_date}")
    print(f"[WARNING] Rows with NULL end_date_key: {null_end_date}")
    
    # Filter out rows with NULL foreign keys (required for fact table)
    print("[INFO] Filtering out rows with NULL foreign keys...")
    fact_df_valid = fact_df.filter(
        col("customer_key").isNotNull() &
        col("car_key").isNotNull() &
        col("pickup_location_key").isNotNull() &
        col("dropoff_location_key").isNotNull() &
        col("start_date_key").isNotNull() &
        col("end_date_key").isNotNull()
    )
    valid_count = fact_df_valid.count()
    filtered_count = final_count - valid_count
    print(f"[INFO] Valid rows after filtering: {valid_count} (filtered out {filtered_count} rows with NULL foreign keys)")
    
    if valid_count == 0:
        print("[ERROR] No valid rows to write after filtering! Exiting.")
        return

    # Final projection: columns for fact table load
    print("[INFO] Preparing final fact table projection...")
    fact_df_final = fact_df_valid.select(
        "rental_id",
        "customer_key",
        "car_key",
        "pickup_location_key",
        "dropoff_location_key",
        "start_date_key",
        "end_date_key",
        "amount",
        "quantity",
        "rental_duration_days",
        "total_rental_amount",
        "average_daily_rental_amount",
        "is_long_rental"
    )
    
    # Show sample data for debugging
    print("[INFO] Sample of data to be written:")
    fact_df_final.show(5, truncate=False)

    # Write fact records to Snowflake (append mode)
    print(f"[INFO] Writing {valid_count} rows to Snowflake table rentals_fact...")
    try:
        fact_df_final.write \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**snowflake_options) \
            .option("dbtable", "rentals_fact") \
            .mode("append") \
            .save()
        print(f"[SUCCESS] Successfully wrote {valid_count} rows to rentals_fact table!")
    except Exception as e:
        print(f"[ERROR] Failed to write to Snowflake: {str(e)}")
        print(f"[ERROR] Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise
    
    print(f"[INFO] Job completed successfully for date: {data_date}")

if __name__ == "__main__":
    # CLI: expects --date yyyymmdd
    parser = argparse.ArgumentParser(description='Process date argument')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    args = parser.parse_args()
     
    process_car_rental_data(args.date)

# Example: process_car_rental_data("20250903")
