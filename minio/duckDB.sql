-- Check if table exists in PostgreSQL
SELECT
    table_schema,
    table_name,
    'Table exists' AS status
FROM information_schema.tables
WHERE table_schema = 'silver' AND table_name = 'pharmacy_sales';

-- Show data statistics
SELECT
    'Data Statistics' AS info,
    COUNT(*) AS total_rows,
    MIN(year) AS min_year,
    MAX(year) AS max_year,
    SUM(sales) AS total_sales,
    COUNT(DISTINCT distributor) AS unique_distributors
FROM silver.pharmacy_sales;

-- Show sample data
SELECT * FROM silver.pharmacy_sales LIMIT 10;

-- Show all data (be careful with large datasets!)
SELECT * FROM silver.pharmacy_sales;

-- Top distributors
SELECT 
    distributor,
    COUNT(*) AS total_records,
    SUM(sales) AS total_sales,
    AVG(sales) AS avg_sales
FROM silver.pharmacy_sales
GROUP BY distributor
ORDER BY total_sales DESC
LIMIT 10;

-- Monthly sales trend
SELECT 
    year,
    month,
    COUNT(*) AS transactions,
    SUM(sales) AS total_sales,
    AVG(sales) AS avg_sales
FROM silver.pharmacy_sales
GROUP BY year, month
ORDER BY year, month;

SELECT *
FROM read_parquet('s3://silver/pharmacy_sales_20260112_210850_*.parquet');