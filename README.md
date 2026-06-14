# 📊 AWS S3 → Glue ETL → Athena Analytics Pipeline

## Architecture
```
Raw CSV File
      ↓  Upload
Amazon S3 (Source Bucket)
      ↓
AWS Glue PySpark ETL Job
      ↓  Clean + Transform + Convert
Amazon S3 (Target Bucket - Parquet)
      ↓
AWS Glue Data Catalog (schema registration)
      ↓
Amazon Athena SQL Queries
```

## What This Pipeline Does
- Uploads raw dirty CSV data to S3 source bucket
- AWS Glue PySpark job reads, cleans, and transforms data
- Output written as **Parquet** format (columnar, compressed)
- AWS Glue Data Catalog registers the table schema
- Amazon Athena queries clean Parquet data using SQL
- Demonstrates partition pruning and cost-efficient querying

## AWS Services Used
| Service | Purpose |
|---|---|
| Amazon S3 | Raw data storage + Parquet output |
| AWS Glue | PySpark ETL processing |
| AWS Glue Data Catalog | Schema registration |
| Amazon Athena | SQL analytics on Parquet |
| AWS IAM | Least-privilege role access |

## Why Parquet over CSV?
| Feature | CSV | Parquet |
|---|---|---|
| File size | Large | 5-10x smaller (compressed) |
| Read speed | Reads all columns | Reads only needed columns |
| Schema | No schema stored | Schema embedded |
| Athena cost | Higher (more data scanned) | Lower (less data scanned) |
| AWS best practice | ❌ | ✅ |

## Glue PySpark ETL Code
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

input_path  = "s3://source-bucket/raw_data.csv"
output_path = "s3://target-bucket/clean_data/"

df = spark.read.option("header", "true").csv(input_path)

# Remove duplicates
df = df.dropDuplicates()

# Remove null rows
df = df.na.drop()

# Trim all columns
for col_name in df.columns:
    df = df.withColumn(col_name, trim(col(col_name)))

# Cast salary to integer
df = df.withColumn("salary", col("salary").cast(IntegerType()))
df = df.na.drop(subset=["salary"])

# Write as Parquet
df.write.mode("overwrite").parquet(output_path)
print(f"Clean data written to: {output_path}")
```

## Athena Queries
```sql
-- Create external table pointing to Parquet
CREATE EXTERNAL TABLE employees_clean (
    id         STRING,
    name       STRING,
    salary     INT,
    department STRING
)
STORED AS PARQUET
LOCATION 's3://target-bucket/clean_data/';

-- Query clean data
SELECT * FROM employees_clean;

-- Department analytics
SELECT department,
       COUNT(*)    AS headcount,
       AVG(salary) AS avg_salary,
       MAX(salary) AS max_salary
FROM employees_clean
GROUP BY department
ORDER BY avg_salary DESC;
```

## IAM Roles Created
- **Glue role:** AmazonS3FullAccess + AWSGlueServiceRole + CloudWatchLogsFullAccess
- Least-privilege: Glue can only access specific S3 buckets

## Key Learnings
- Parquet is the preferred format in AWS data lakes
- AWS Glue Data Catalog enables schema-on-read for Athena
- IAM least-privilege principle for secure pipeline design
- Partition pruning reduces Athena query cost significantly

## Author
**Barathwaj K G** | AWS Data Engineer
[LinkedIn](https://linkedin.com/in/barathwaj-kg) | Tiruchirappalli, TN | Immediate Joiner
