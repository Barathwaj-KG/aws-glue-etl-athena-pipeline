-- ═══════════════════════════════════════
-- Project 1: Athena Queries
-- S3 → Glue ETL → Athena Analytics
-- Author: Barathwaj K G
-- ═══════════════════════════════════════

-- Step 1: Drop old table if exists
DROP TABLE IF EXISTS employees_clean;

-- Step 2: Create external table on Parquet output
CREATE EXTERNAL TABLE IF NOT EXISTS employees_clean (
    id         STRING,
    name       STRING,
    salary     INT,
    department STRING
)
STORED AS PARQUET
LOCATION 's3://your-target-bucket/clean_data/';

-- Step 3: View all clean records
SELECT * FROM employees_clean;

-- Step 4: Department analytics
SELECT
    department,
    COUNT(*)         AS headcount,
    AVG(salary)      AS avg_salary,
    MAX(salary)      AS max_salary,
    MIN(salary)      AS min_salary
FROM employees_clean
GROUP BY department
ORDER BY avg_salary DESC;

-- Step 5: Employees earning above company average
SELECT name, salary, department
FROM employees_clean
WHERE salary > (SELECT AVG(salary) FROM employees_clean)
ORDER BY salary DESC;

-- Step 6: Top earner per department
SELECT * FROM (
    SELECT name, department, salary,
           RANK() OVER (PARTITION BY department
                        ORDER BY salary DESC) AS rnk
    FROM employees_clean
) t WHERE rnk = 1;
