from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, initcap, regexp_replace
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

# Update these paths to your bucket names
input_path  = "s3://your-source-bucket/raw_data.csv"
output_path = "s3://your-target-bucket/clean_data/"

# Read raw CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(input_path)

print(f"Raw row count: {df.count()}")
df.show(5)

# Step 1: Remove fully empty rows
df = df.na.drop(how="all")

# Step 2: Remove duplicates
df = df.dropDuplicates()

# Step 3: Trim whitespace from all columns
for col_name in df.columns:
    df = df.withColumn(col_name, trim(col(col_name)))

# Step 4: Clean name column
df = df.withColumn("name",
    initcap(trim(regexp_replace(col("name"), "[^A-Za-z ]", ""))))

# Step 5: Clean salary — remove Rs. INR symbols etc
df = df.withColumn("salary",
    regexp_replace(col("salary"), "[^0-9]", ""))
df = df.withColumn("salary", col("salary").cast(IntegerType()))
df = df.na.drop(subset=["salary"])

print(f"Clean row count: {df.count()}")
df.show(10)

# Step 6: Write as Parquet to target S3
df.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Done! Clean Parquet written to: {output_path}")
