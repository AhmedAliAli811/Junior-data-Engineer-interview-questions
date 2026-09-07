# Junior Data Engineer Interview — Complete Question & Answer Bank

> A practical interview preparation guide covering SQL, Python, data engineering fundamentals, ETL/ELT, data modeling, Spark, Airflow, cloud, debugging, behavioral questions, and English interview questions.

---

# Index

1. [SQL — Theory](#1-sql--theory)
2. [SQL — Practical](#2-sql--practical)
3. [SQL — Window Functions](#3-sql--window-functions)
4. [SQL — Performance](#4-sql--performance)
5. [Incremental Loading](#5-incremental-loading)
6. [Python — Theory](#6-python--theory)
7. [Python — Practical](#7-python--practical)
8. [Data Engineering Fundamentals](#8-data-engineering-fundamentals)
9. [ETL/ELT & Pipeline Design](#9-etlelt--pipeline-design)
10. [Idempotency](#10-idempotency)
11. [Data Quality](#11-data-quality)
12. [Data Modeling](#12-data-modeling)
13. [Apache Spark / PySpark](#13-apache-spark--pyspark)
14. [Spark — Practical & Performance](#14-spark--practical--performance)
15. [Apache Airflow](#15-apache-airflow)
16. [Azure](#16-azure)
17. [AWS](#17-aws)
18. [Real-World Debugging Scenarios](#18-real-world-debugging-scenarios)
19. [Pipeline Design Interview](#19-pipeline-design-interview)
20. [Take-Home Assignment](#20-take-home-assignment)
21. [Behavioral Interview — STAR](#21-behavioral-interview--star)
22. [Experience-Based Questions](#22-experience-based-questions)
23. [English Technical Interview](#23-english-technical-interview)
24. [Recruiter / HR Questions](#24-recruiter--hr-questions)
25. [Questions to Ask the Interviewer](#25-questions-to-ask-the-interviewer)

---

# 1. SQL — Theory

## Q1. What is the difference between `WHERE` and `HAVING`?

**Answer:** `WHERE` filters individual rows before grouping, while `HAVING` filters groups after `GROUP BY`.

```sql
SELECT customer_id, SUM(amount) AS total_sales
FROM sales
WHERE order_date >= '2026-01-01'
GROUP BY customer_id
HAVING SUM(amount) > 10000;
```

## Q2. What is the difference between `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `FULL OUTER JOIN`?

**Answer:**
- `INNER JOIN`: only matching rows from both tables.
- `LEFT JOIN`: all rows from the left table plus matching rows from the right.
- `RIGHT JOIN`: all rows from the right table plus matching rows from the left.
- `FULL OUTER JOIN`: all rows from both tables; unmatched values become `NULL`.

## Q3. What is a Primary Key?

**Answer:** A column or set of columns that uniquely identifies each row in a table. It must be unique and cannot contain `NULL`.

## Q4. What is a Foreign Key?

**Answer:** A column or set of columns that references a key in another table. It is used to maintain relationships and referential integrity.

## Q5. What is the difference between `DELETE`, `TRUNCATE`, and `DROP`?

**Answer:**
- `DELETE`: removes selected rows and can use `WHERE`.
- `TRUNCATE`: removes all rows quickly and generally has less logging overhead.
- `DROP`: removes the database object itself, including its definition.

Exact transaction/logging behavior depends on the database engine.

## Q6. What is `NULL` in SQL?

**Answer:** `NULL` represents an unknown or missing value. It is not equal to zero, an empty string, or another `NULL`.

Use `IS NULL` / `IS NOT NULL` rather than `= NULL`.

## Q7. What is the difference between `COUNT(*)` and `COUNT(column)`?

**Answer:** `COUNT(*)` counts rows. `COUNT(column)` counts non-`NULL` values in that column.

## Q8. What is the difference between `UNION` and `UNION ALL`?

**Answer:** `UNION` combines result sets and removes duplicate rows. `UNION ALL` keeps duplicates and is usually faster because it does not perform duplicate elimination.

## Q9. What is a CTE?

**Answer:** A Common Table Expression is a named temporary result set defined with `WITH`. It improves readability and can simplify complex queries.

```sql
WITH customer_sales AS (
    SELECT customer_id, SUM(amount) AS total_sales
    FROM sales
    GROUP BY customer_id
)
SELECT *
FROM customer_sales
WHERE total_sales > 10000;
```

## Q10. CTE vs subquery?

**Answer:** Both can express similar logic. A CTE is often easier to read and reuse within the same statement, while a subquery can be convenient for small localized logic. Performance depends on the database optimizer and query.

## Q11. What is a view?

**Answer:** A view is a stored SQL query that behaves like a virtual table. It can simplify access to complex queries and provide an abstraction layer.

## Q12. What is an index?

**Answer:** An index is a data structure that helps the database find rows faster without scanning the entire table.

## Q13. Can indexes make things slower?

**Answer:** Yes. Indexes consume storage and must be maintained during `INSERT`, `UPDATE`, and `DELETE`. Too many or poorly chosen indexes can hurt write performance.

## Q14. What is normalization?

**Answer:** Normalization organizes data into related tables to reduce redundancy and improve consistency.

## Q15. What is denormalization?

**Answer:** Denormalization intentionally introduces some redundancy to simplify queries or improve read performance.

## Q16. What is a transaction?

**Answer:** A transaction is a logical unit of database work that should be committed as a whole or rolled back according to the database's transaction rules.

## Q17. What are ACID properties?

**Answer:**
- **Atomicity:** all-or-nothing execution.
- **Consistency:** transactions preserve database rules.
- **Isolation:** concurrent transactions are controlled so they do not improperly interfere.
- **Durability:** committed changes survive failures.

## Q18. What is a deadlock?

**Answer:** A deadlock occurs when two or more transactions wait for locks held by one another, so none can proceed. Databases usually detect and terminate one transaction.

## Q19. What is a full table scan?

**Answer:** The database reads a large portion or all of a table instead of efficiently locating a smaller set of rows through an index or another access path.

---

# 2. SQL — Practical

## Q20. Find total sales for each customer.

**Answer:**

```sql
SELECT
    customer_id,
    SUM(amount) AS total_sales
FROM sales
GROUP BY customer_id;
```

## Q21. Find customers whose total sales exceed 100,000.

**Answer:**

```sql
SELECT
    customer_id,
    SUM(amount) AS total_sales
FROM sales
GROUP BY customer_id
HAVING SUM(amount) > 100000;
```

## Q22. Find the number of orders per day.

**Answer:**

```sql
SELECT
    CAST(order_date AS DATE) AS order_day,
    COUNT(*) AS order_count
FROM orders
GROUP BY CAST(order_date AS DATE)
ORDER BY order_day;
```

## Q23. Find the average order value per customer.

**Answer:**

```sql
SELECT
    customer_id,
    AVG(amount) AS avg_order_value
FROM orders
GROUP BY customer_id;
```

## Q24. Find the top 5 customers by revenue.

**Answer:**

```sql
SELECT
    customer_id,
    SUM(amount) AS revenue
FROM orders
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 5;
```

> SQL Server uses `TOP (5)` instead of `LIMIT 5`.

## Q25. Find customers who never placed an order.

**Answer:**

```sql
SELECT c.customer_id
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

## Q26. Find orders that don't have a matching customer.

**Answer:**

```sql
SELECT o.*
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

## Q27. How can a `LEFT JOIN` accidentally behave like an `INNER JOIN`?

**Answer:** If you filter a nullable right-table column in the `WHERE` clause, unmatched rows are removed.

Problem:

```sql
SELECT *
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.status = 'PAID';
```

Better when the filter belongs to the join:

```sql
SELECT *
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
   AND o.status = 'PAID';
```

## Q28. You have duplicate rows after a JOIN. How would you investigate?

**Answer:**
1. Check the expected relationship/cardinality.
2. Check whether the join keys are unique.
3. Count duplicates in each table.
4. Validate the join condition.
5. Check whether the relationship is one-to-many or many-to-many.
6. Compare row counts before and after the join.
7. Fix the data/model/query rather than blindly applying `DISTINCT`.

---

# 3. SQL — Window Functions

## Q29. What is a window function?

**Answer:** A window function calculates a value across related rows while preserving the individual rows, unlike `GROUP BY`, which collapses rows into groups.

## Q30. What is the difference between `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()`?

**Answer:**
- `ROW_NUMBER()` gives every row a unique sequential number.
- `RANK()` gives ties the same rank and leaves gaps.
- `DENSE_RANK()` gives ties the same rank without gaps.

Example values: `100, 100, 90`

```text
ROW_NUMBER: 1, 2, 3
RANK:       1, 1, 3
DENSE_RANK: 1, 1, 2
```

## Q31. Find the highest-paid employee in each department.

**Answer:**

```sql
WITH ranked AS (
    SELECT
        employee_id,
        department_id,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY department_id
            ORDER BY salary DESC
        ) AS rn
    FROM employees
)
SELECT *
FROM ranked
WHERE rn = 1;
```

## Q32. Find the top 3 employees in each department.

**Answer:**

```sql
WITH ranked AS (
    SELECT
        employee_id,
        department_id,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY department_id
            ORDER BY salary DESC
        ) AS rn
    FROM employees
)
SELECT *
FROM ranked
WHERE rn <= 3;
```

## Q33. Find the latest order for every customer.

**Answer:**

```sql
WITH ranked AS (
    SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS rn
    FROM orders o
)
SELECT *
FROM ranked
WHERE rn = 1;
```

## Q34. Find the previous order for every customer.

**Answer:**

```sql
SELECT
    customer_id,
    order_id,
    order_date,
    LAG(order_date) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS previous_order_date
FROM orders;
```

## Q35. Calculate a running total of sales.

**Answer:**

```sql
SELECT
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales;
```

## Q36. Calculate a 7-day rolling average.

**Answer:**

If the data has exactly one row per day:

```sql
SELECT
    order_date,
    revenue,
    AVG(revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_avg
FROM daily_sales;
```

For real-world data with missing dates, first create a calendar/date series and aggregate to one row per day.

## Q37. Detect duplicate records using `ROW_NUMBER()`.

**Answer:**

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, order_date, amount
            ORDER BY created_at
        ) AS rn
    FROM sales
)
SELECT *
FROM ranked
WHERE rn > 1;
```

---

# 4. SQL — Performance

## Q38. A query that used to run in 30 seconds now takes 10 minutes. How would you investigate?

**Answer:**
1. Check whether the data volume increased.
2. Compare the current and previous execution plans.
3. Check indexes and statistics.
4. Look for new joins or filters.
5. Check table scans and expensive operators.
6. Check blocking/locks.
7. Check database/server resources.
8. Check whether a recent code or schema change caused the regression.

## Q39. What is an index seek vs an index scan?

**Answer:** A seek navigates an index to locate relevant rows. A scan reads many or all index entries. A scan is not automatically bad; it can be appropriate when a large percentage of rows is needed.

## Q40. Why might a database ignore an index?

**Answer:** Possible reasons include low selectivity, stale statistics, an unsuitable index, implicit conversions, functions on columns, or the optimizer estimating that a scan is cheaper.

## Q41. How can functions on indexed columns hurt performance?

**Answer:** Applying a function can make the predicate less searchable, potentially preventing an efficient index access path.

Instead of:

```sql
WHERE YEAR(order_date) = 2026
```

a range predicate is often better:

```sql
WHERE order_date >= '2026-01-01'
  AND order_date <  '2027-01-01'
```

## Q42. What is partitioning?

**Answer:** Partitioning divides a large table or dataset into logical partitions, often by date or another key, so queries and maintenance can work with smaller portions of the data.

## Q43. Indexing vs partitioning?

**Answer:** Indexes improve row lookup/access paths. Partitioning divides the physical/logical data into larger chunks. They solve different problems and can be used together.

---

# 5. Incremental Loading

## Q44. What is the difference between full load and incremental load?

**Answer:** A full load processes the entire source dataset. An incremental load processes only new or changed records since the previous successful run.

## Q45. Why avoid a full refresh every day?

**Answer:** As data grows, full refreshes consume more compute, I/O, network bandwidth, and time. They also increase the operational cost and failure window.

## Q46. How would you implement an incremental load?

**Answer:** Common approaches:
- `updated_at` watermark.
- Increasing ID/key.
- CDC.
- Source-system change flags.
- File partitions or ingestion timestamps.

Example:

```sql
SELECT *
FROM source
WHERE updated_at > :last_successful_watermark;
```

Then persist the new watermark only after the load succeeds.

## Q47. What is a watermark?

**Answer:** A watermark is a stored progress marker representing how far an incremental pipeline has successfully processed, such as the maximum source timestamp or ID.

## Q48. What problems can happen with `updated_at > last_run_time`?

**Answer:** Timestamp precision, equal timestamps, late-arriving updates, clock differences, timezone issues, and records committed after the watermark can cause data to be missed.

A robust pipeline may use a small overlap window and deduplicate/upsert downstream.

## Q49. What is CDC?

**Answer:** Change Data Capture identifies inserts, updates, and sometimes deletes from a source so downstream systems can process changes incrementally.

---

# 6. Python — Theory

## Q50. List vs tuple?

**Answer:** Lists are mutable; tuples are immutable. Tuples can be useful for fixed collections and can be hashable when all contained values are hashable.

## Q51. List vs set?

**Answer:** Lists preserve ordered elements and allow duplicates. Sets store unique hashable elements and are optimized for membership checks.

## Q52. Why are dictionaries useful in data engineering?

**Answer:** Dictionaries provide key-value lookup and are very useful for configuration, record representation, mappings, joins, counters, and aggregations.

## Q53. What is `defaultdict`?

**Answer:** `collections.defaultdict` is a dictionary that creates a default value when a missing key is accessed.

```python
from collections import defaultdict

totals = defaultdict(float)

for row in orders:
    totals[row["customer_id"]] += row["amount"]
```

## Q54. What is a generator?

**Answer:** A generator produces values lazily, one at a time, rather than storing the entire sequence in memory. This is useful for large files and streams.

## Q55. What is exception handling?

**Answer:** Exception handling lets a program detect and handle runtime errors without necessarily terminating unexpectedly.

```python
try:
    process_file()
except ValueError as e:
    log_error(e)
finally:
    cleanup()
```

## Q56. What is `None`?

**Answer:** `None` represents the absence of a value in Python. Use `is None` and `is not None` for checks.

## Q57. Mutable vs immutable objects?

**Answer:** Mutable objects can be changed after creation, such as lists and dictionaries. Immutable objects cannot be changed in place, such as strings, integers, and tuples.

---

# 7. Python — Practical

## Q58. Calculate total sales per customer.

**Answer:**

```python
from collections import defaultdict

orders = [
    {"customer": "Ahmed", "amount": 100},
    {"customer": "Ali", "amount": 200},
    {"customer": "Ahmed", "amount": 300},
]

totals = defaultdict(float)

for order in orders:
    totals[order["customer"]] += order["amount"]

print(dict(totals))
```

Output:

```text
{'Ahmed': 400.0, 'Ali': 200.0}
```

## Q59. Remove duplicates and handle missing names.

**Answer:**

```python
data = [
    {"id": 1, "name": "Ahmed"},
    {"id": 2, "name": None},
    {"id": 1, "name": "Ahmed"},
]

seen = set()
cleaned = []

for row in data:
    if row["id"] in seen:
        continue

    seen.add(row["id"])

    if row["name"] is None:
        row["name"] = "Unknown"

    cleaned.append(row)
```

## Q60. How would you process a huge CSV that does not fit in memory?

**Answer:** Process it in chunks or stream it row by row. With pandas:

```python
import pandas as pd

for chunk in pd.read_csv("large.csv", chunksize=100_000):
    transform(chunk)
    load(chunk)
```

For very large distributed datasets, Spark or another distributed processing system may be more appropriate.

## Q61. How do you handle dates and timezones?

**Answer:** Parse timestamps explicitly, normalize them to a consistent timezone such as UTC for storage/processing, and convert to local time only when needed for presentation.

## Q62. How would you load data into PostgreSQL from Python?

**Answer:** Use a PostgreSQL driver such as `psycopg` and parameterized SQL. For large loads, use bulk-loading mechanisms such as PostgreSQL `COPY` when appropriate.

Never build SQL by concatenating untrusted values.

## Q63. Your script fails halfway through loading 1 million records. How do you restart without duplicating data?

**Answer:** Make the load idempotent. Options include:
- Stage data first.
- Use a deterministic business key.
- Enforce unique constraints.
- Use `INSERT ... ON CONFLICT`/upsert where appropriate.
- Track successful batches.
- Commit in controlled transactions.
- Make retries safe.

---

# 8. Data Engineering Fundamentals

## Q64. What is Data Engineering?

**Answer:** Data Engineering is the discipline of designing and operating systems that ingest, transform, store, validate, and make data available reliably for analytics, applications, and machine learning.

## Q65. What is ETL?

**Answer:** Extract data from sources, transform it, then load it into the target system.

## Q66. What is ELT?

**Answer:** Extract data, load it into the target platform, then perform transformations there.

## Q67. ETL vs ELT?

**Answer:** ETL transforms before loading, often in a separate processing layer. ELT loads raw or lightly processed data first and uses the target warehouse/lakehouse compute for transformation.

## Q68. What is a data pipeline?

**Answer:** A sequence of automated steps that moves and processes data from sources to destinations while handling scheduling, validation, errors, retries, and monitoring.

## Q69. What is a data warehouse?

**Answer:** A system optimized for analytical workloads, typically storing structured, modeled data for reporting and BI.

## Q70. What is a data lake?

**Answer:** A storage system that can hold large amounts of raw and processed data in multiple formats, often using inexpensive object storage.

## Q71. Data warehouse vs data lake?

**Answer:** Warehouses emphasize structured, governed analytical data and SQL performance. Lakes emphasize flexible, scalable storage of raw and varied data. Modern lakehouses combine aspects of both.

## Q72. What is a data lakehouse?

**Answer:** A lakehouse is an architecture that keeps data in scalable lake storage while adding warehouse-like capabilities such as ACID transactions, schema management, governance, and analytical performance.

## Q73. Batch vs streaming?

**Answer:** Batch processes data in scheduled or accumulated groups. Streaming processes events continuously or with very low latency.

Choose based on business latency requirements, complexity, cost, and source characteristics.

## Q74. OLTP vs OLAP?

**Answer:** OLTP systems support frequent operational transactions. OLAP systems are optimized for analytical queries over larger datasets.

---

# 9. ETL/ELT & Pipeline Design

## Q75. Explain this pipeline:

```text
CSV
 ↓
Python / Spark
 ↓
Staging
 ↓
Transformations
 ↓
Data Warehouse
 ↓
Power BI
```

**Answer:** Files are ingested into a staging area, validated and transformed, loaded into warehouse tables, and then consumed by BI/reporting tools.

## Q76. Where should data validation happen?

**Answer:** At multiple stages:
- During ingestion for file/schema validation.
- During transformation for business rules.
- Before warehouse publication for integrity and reconciliation.
- After loading for row counts and metric checks.

## Q77. Where should duplicates be handled?

**Answer:** Preferably as close to ingestion as practical, but duplicate prevention should also be enforced at the target using keys/constraints or deterministic merge logic.

## Q78. What happens if a pipeline fails?

**Answer:** Capture the error, mark the run as failed, alert the appropriate team, preserve enough state/logs for diagnosis, and retry only if the operation is safe to retry.

## Q79. How would you handle schema changes?

**Answer:** Detect schema changes, validate them against an expected contract, decide whether the change is backward compatible, update transformations deliberately, and monitor for unexpected columns/type changes.

## Q80. How would you handle late-arriving data?

**Answer:** Allow reprocessing of affected partitions/windows or perform targeted upserts. The design should support corrections rather than assuming data always arrives on time.

---

# 10. Idempotency

## Q81. What does idempotency mean in data pipelines?

**Answer:** Running the same pipeline operation multiple times with the same input should produce the same intended final state, rather than creating duplicates or inconsistent results.

## Q82. Why is idempotency important?

**Answer:** Pipelines fail and retry. Without idempotency, a retry can duplicate records or corrupt downstream results.

## Q83. Give an example of a non-idempotent pipeline.

**Answer:** A job that blindly executes:

```sql
INSERT INTO fact_sales
SELECT *
FROM staging_sales;
```

every time it runs can duplicate the same sales if the staging data has not changed.

## Q84. How would you make it idempotent?

**Answer:** Use deterministic keys, unique constraints, merge/upsert logic, partition replacement, staging tables, transaction boundaries, and run metadata.

---

# 11. Data Quality

## Q85. What is data quality?

**Answer:** Data quality is the degree to which data is accurate, complete, consistent, valid, timely, unique, and fit for its intended use.

## Q86. What checks would you implement?

**Answer:**
- Null checks.
- Duplicate checks.
- Data type/schema checks.
- Range checks.
- Referential integrity.
- Uniqueness.
- Row-count reconciliation.
- Business-rule validation.
- Freshness checks.

## Q87. What if 10% of today's records are invalid?

**Answer:** It depends on the business requirement. I would quarantine/reject invalid records, log the reasons, alert the team, and decide whether to fail the whole run or publish valid records based on agreed data-quality thresholds.

## Q88. How would you design an error/rejection table?

**Answer:** Store enough information to identify the pipeline run and source record, such as:

```text
RunID
PackageName
ErrorDate
SourceRecordKey
ErrorReason
OriginalValues
```

This allows troubleshooting and possible reprocessing.

---

# 12. Data Modeling

## Q89. What is a fact table?

**Answer:** A fact table stores measurable business events, such as sales, transactions, shipments, or clicks, usually with foreign keys to dimensions.

## Q90. What is a dimension table?

**Answer:** A dimension table stores descriptive context, such as customer, product, date, or location attributes.

## Q91. What is a star schema?

**Answer:** A central fact table connected directly to denormalized dimension tables.

## Q92. What is a snowflake schema?

**Answer:** A dimensional model where dimensions are further normalized into related tables.

## Q93. What is grain?

**Answer:** Grain defines exactly what one row in a fact table represents.

Example:

> One row represents one product on one sales order.

## Q94. Why is grain important?

**Answer:** It determines what can be correctly measured and prevents accidental double counting.

## Q95. What is a surrogate key?

**Answer:** A system-generated key used to identify a dimension record, independent of the source-system business key.

## Q96. What is SCD Type 1?

**Answer:** Type 1 overwrites the old dimension value. History is not preserved.

## Q97. What is SCD Type 2?

**Answer:** Type 2 preserves history by creating a new dimension row when tracked attributes change.

Typical columns:

```text
CustomerKey
CustomerID
Name
City
StartDate
EndDate
IsCurrent
```

## Q98. When would you use SCD Type 2?

**Answer:** When historical context matters, such as analyzing sales according to the customer's region at the time of the sale.

---

# 13. Apache Spark / PySpark

## Q99. What is Apache Spark?

**Answer:** Spark is a distributed data processing engine used for large-scale batch and streaming workloads.

## Q100. Why is Spark fast?

**Answer:** It distributes computation across multiple machines, optimizes execution, supports parallel processing, and can keep intermediate data in memory when beneficial.

## Q101. What is an RDD?

**Answer:** An RDD is Spark's lower-level distributed collection abstraction. It is fault-tolerant and partitioned across a cluster.

## Q102. What is a DataFrame?

**Answer:** A distributed dataset organized into named columns, with a schema and SQL-style operations. Spark can optimize DataFrame operations using its query optimizer.

## Q103. RDD vs DataFrame?

**Answer:** DataFrames provide higher-level structured operations and generally allow Spark's optimizer to generate more efficient execution plans. RDDs provide lower-level control and are useful for cases that do not fit naturally into structured APIs.

## Q104. What is a transformation?

**Answer:** A lazy operation that creates a new dataset from an existing one, such as `select`, `filter`, or `map`.

## Q105. What is an action?

**Answer:** An operation that triggers execution, such as `count`, `collect`, or writing data.

## Q106. What is lazy evaluation?

**Answer:** Spark does not immediately execute transformations. It builds a logical execution plan and executes it when an action requires a result.

## Q107. What is a narrow transformation?

**Answer:** A transformation where each output partition depends on a small number of input partitions, generally without a full shuffle.

Examples include `filter` and many `map` operations.

## Q108. What is a wide transformation?

**Answer:** A transformation where data must be redistributed between partitions, causing a shuffle.

Examples include many `groupBy`, `join`, and `distinct` operations.

## Q109. What is a shuffle?

**Answer:** A redistribution of data across partitions, usually involving network I/O and disk/memory overhead.

## Q110. Why is shuffle expensive?

**Answer:** It can involve serialization, network transfer, sorting, disk spilling, and synchronization across executors.

## Q111. What is data skew?

**Answer:** Data skew occurs when some partition keys contain much more data than others, causing uneven task workloads.

## Q112. How can you handle data skew?

**Answer:** Depending on the case:
- Broadcast a small table.
- Use salting for hot keys.
- Pre-aggregate.
- Repartition appropriately.
- Filter/handle pathological keys.
- Use Spark's skew-related optimizations where available.

## Q113. What is a partition?

**Answer:** A partition is a chunk of distributed data processed by a Spark task.

## Q114. `repartition()` vs `coalesce()`?

**Answer:** `repartition()` generally performs a full shuffle to redistribute data and can increase or decrease partitions. `coalesce()` is mainly used to reduce partitions with less data movement.

## Q115. What is caching?

**Answer:** Caching stores computed data so repeated operations can reuse it instead of recomputing it. Cache only when the dataset is reused enough to justify the memory/storage cost.

## Q116. What are Spark driver and executors?

**Answer:** The driver coordinates the Spark application and creates the execution plan. Executors run tasks and hold data for the application.

---

# 14. Spark — Practical & Performance

## Q117. A Spark task takes 30 minutes while others finish in 2 minutes. What could be happening?

**Answer:** A common cause is data skew. One partition may contain a disproportionately large amount of data. I would inspect partition sizes, the query plan, shuffle stages, and the key distribution.

## Q118. How would you identify data skew?

**Answer:** Look for highly uneven partition sizes and task durations in the Spark UI. Then inspect the distribution of the keys involved in joins/grouping.

## Q119. What is a broadcast join?

**Answer:** Spark sends a small table to each executor so the large table can be joined locally without shuffling the large table across the cluster.

## Q120. What happens if you broadcast a huge table?

**Answer:** Executors may run out of memory, causing severe performance problems or failures. Broadcasting should be limited to genuinely small datasets that fit safely in executor memory.

## Q121. How can you reduce unnecessary shuffles?

**Answer:** Filter early, select only required columns, use appropriate partitioning, avoid unnecessary `distinct`/`groupBy`, leverage broadcast joins for small tables, and inspect the physical plan.

---

# 15. Apache Airflow

## Q122. What is Apache Airflow?

**Answer:** Airflow is a workflow orchestration platform used to define, schedule, monitor, and manage dependencies between tasks.

## Q123. What is a DAG?

**Answer:** A Directed Acyclic Graph representing workflow tasks and their dependencies. It has no cycles.

## Q124. What is a task?

**Answer:** A single unit of work in an Airflow workflow.

## Q125. What is an operator?

**Answer:** A reusable task template that defines a particular type of work, such as running Python code or executing SQL.

## Q126. What happens when an Airflow task fails?

**Answer:** Airflow records the failure, applies retry settings if configured, and can trigger alerts or downstream behavior depending on the DAG design.

## Q127. What is backfilling?

**Answer:** Running a workflow for historical scheduled periods that were not previously processed.

## Q128. Retry vs rerun?

**Answer:** A retry is an automated attempt after a failure according to task retry settings. A rerun is an intentional execution initiated again, often manually or through a new scheduling operation.

## Q129. How would you monitor an Airflow pipeline?

**Answer:** Use the Airflow UI, task logs, task duration/failure metrics, alerts, and external monitoring where appropriate.

## Q130. How would you prevent two instances of the same pipeline from running simultaneously?

**Answer:** Configure DAG/task concurrency controls and design the underlying pipeline to be safe if concurrent execution occurs. Idempotency is still important.

---

# 16. Azure

## Q131. What is Azure Data Factory?

**Answer:** A managed data integration and orchestration service used to build pipelines that move and transform data.

## Q132. What is ADLS?

**Answer:** Azure Data Lake Storage is scalable cloud object storage designed for large-scale data workloads.

## Q133. What is Azure Databricks?

**Answer:** A managed analytics platform based on Apache Spark, commonly used for distributed data processing and lakehouse workloads.

## Q134. What is Azure Synapse?

**Answer:** An Azure analytics service combining data warehousing and big-data analytics capabilities.

## Q135. Design a basic Azure data pipeline.

**Answer:**

```text
Source
  ↓
ADF
  ↓
ADLS
  ↓
Databricks / Spark
  ↓
Synapse / Warehouse
  ↓
Power BI
```

ADF handles orchestration and movement, ADLS provides scalable storage, Databricks handles distributed transformation, and the warehouse serves analytical workloads.

---

# 17. AWS

## Q136. What is Amazon S3?

**Answer:** S3 is AWS object storage commonly used as a durable, scalable data lake storage layer.

## Q137. What is AWS Glue?

**Answer:** Glue is a managed data integration service that can run ETL jobs and maintain metadata through the Glue Data Catalog.

## Q138. What is Athena?

**Answer:** Athena is a serverless query service that can query data in S3 using SQL.

## Q139. What is Redshift?

**Answer:** Redshift is AWS's managed analytical data warehouse.

## Q140. S3 vs Redshift?

**Answer:** S3 is object storage and can store raw/processed data in many formats. Redshift is an analytical warehouse optimized for structured analytical queries.

## Q141. Why is Parquet preferred over CSV for analytics?

**Answer:** Parquet is columnar, supports compression and efficient column pruning, and preserves schema/types better than plain CSV. This can reduce I/O and improve analytical query performance.

## Q142. Design a basic AWS pipeline.

**Answer:**

```text
API / Source
    ↓
S3
    ↓
Glue / Spark / Python
    ↓
Redshift
    ↓
BI
```

---

# 18. Real-World Debugging Scenarios

## Q143. Your daily pipeline normally takes 20 minutes. Today it took 2 hours. What do you check?

**Answer:**
- Source data volume.
- Data skew.
- Query plans.
- New code changes.
- Partitions.
- Locks.
- Cluster/compute resources.
- Network/storage performance.
- Late-arriving data.
- Recent schema changes.

Start with evidence rather than guessing.

## Q144. The pipeline succeeded, but the dashboard shows wrong numbers. What do you do?

**Answer:**
1. Reproduce the metric independently.
2. Compare source, staging, warehouse, and BI values.
3. Check filters and joins.
4. Check duplicate or missing records.
5. Check business definitions.
6. Check refresh timing/cache.
7. Trace one known record end-to-end.

## Q145. The source has 1 million records but the warehouse has 1.2 million. Why?

**Answer:** Possible causes include duplicate ingestion, one-to-many joins, incorrect merge logic, historical rows from SCD2, reprocessing, or different definitions of what counts as a record. Compare counts at every pipeline stage.

## Q146. The pipeline failed after inserting 70% of the data. What do you do?

**Answer:** First determine whether the load was transactional or partial. Identify exactly what was committed, then use rollback/recovery where possible. Otherwise clean up or reconcile the partial load and rerun using an idempotent process.

## Q147. The source schema changed without informing you. What do you do?

**Answer:** Inspect the exact schema difference, determine compatibility, protect downstream tables from silent corruption, update the ingestion contract/transformation, test it, and add schema-drift detection to prevent recurrence.

## Q148. A stakeholder says yesterday's revenue is wrong. How do you investigate?

**Answer:** Define the exact revenue metric first. Then trace a sample from source to warehouse to dashboard, compare totals and record counts, inspect filters/joins/returns/cancellations, and identify where the first discrepancy appears.

## Q149. A pipeline works in development but fails in production. What do you check?

**Answer:** Environment variables, credentials, permissions, connection strings, package/library versions, paths, data differences, network access, resource limits, configuration, and production-specific schema/data issues.

## Q150. An API sometimes returns HTTP 500. What would you do?

**Answer:** Add controlled retries with exponential backoff, set timeouts, log failures, avoid infinite retries, and make the downstream load idempotent. After repeated failures, alert and fail clearly.

---

# 19. Pipeline Design Interview

## Q151. Design a daily sales data pipeline.

**Answer:**

A reasonable junior-level design:

```text
Source Files / API
       ↓
Raw Storage
       ↓
Validation
       ↓
Staging
       ↓
Transformations
       ↓
Warehouse
       ↓
BI
```

Key design considerations:
- Store raw data for traceability.
- Validate schema and records.
- Use incremental processing.
- Deduplicate using business keys.
- Log every run.
- Quarantine bad records.
- Make retries idempotent.
- Monitor freshness, row counts, failures, and duration.

## Q152. How do you make the pipeline incremental?

**Answer:** Identify a reliable change indicator such as `updated_at`, an increasing ID, CDC, or partitioned files. Store a successful watermark and process only data after that point, with safeguards for late/equal-timestamp records.

## Q153. How do you guarantee idempotency?

**Answer:** Use deterministic keys and upserts/merges or replace complete target partitions. Track pipeline runs and only advance the watermark after successful processing.

## Q154. How do you handle bad records?

**Answer:** Validate records, separate invalid rows into a quarantine/rejection area with an error reason, continue or fail according to business rules, and provide a reprocessing path.

## Q155. How do you monitor the pipeline?

**Answer:** Monitor:
- Success/failure.
- Duration.
- Rows read/inserted/rejected.
- Data freshness.
- Null/duplicate rates.
- Schema changes.
- Resource usage.
- Alerts for abnormal behavior.

---

# 20. Take-Home Assignment

## Q156. You receive `orders.csv`, `customers.csv`, and `products.csv`. What would you build?

**Answer:**

```text
CSV files
   ↓
Ingestion
   ↓
Validation
   ↓
Cleaning
   ↓
Deduplication
   ↓
Transformation / Joins
   ↓
PostgreSQL
   ↓
Data Quality Checks
   ↓
Logging / Monitoring
```

I would also document assumptions, schema, data-quality rules, and how the pipeline can be rerun safely.

## Q157. Why might you choose Pandas?

**Answer:** For moderate datasets that fit comfortably on one machine, Pandas is productive and easy to use. If the data becomes too large for memory or requires distributed processing, I would consider Spark or another scalable processing approach.

## Q158. What if the data becomes 100x larger?

**Answer:** Reassess the architecture. Move toward distributed processing, partitioned columnar storage, incremental processing, scalable object storage, and distributed compute. Avoid loading the entire dataset into memory.

---

# 21. Behavioral Interview — STAR

## Q159. Tell me about a time you discovered a data quality issue.

**Answer structure:**

**Situation:** Explain the business context.

**Task:** Explain what you were responsible for.

**Action:** Focus on what you personally investigated and changed.

**Result:** Quantify the impact when possible.

Example ending:

> “As a result, we identified the root cause, corrected the transformation logic, and added a validation step to prevent the issue from recurring.”

## Q160. Tell me about a time you made a mistake.

**Answer:** Choose a real but manageable mistake. Explain what happened, take ownership, describe the corrective action, and explain what process you changed afterward.

Avoid blaming teammates.

## Q161. Tell me about a time you learned a new technology quickly.

**Answer structure:**

> “I first identified the minimum concepts I needed, followed the official documentation/tutorials, built a small practical example, then applied the technology to the actual problem. I validated my understanding through testing and debugging.”

## Q162. Tell me about a disagreement with a teammate.

**Answer:** Explain the disagreement objectively, show that you listened to the other person's reasoning, compare options using technical/business criteria, agree on a solution, and describe the result.

## Q163. Tell me about a time you worked under a tight deadline.

**Answer:** Explain how you prioritized the essential work, communicated risks early, broke the task into smaller pieces, and delivered the highest-value functionality first.

## Q164. Tell me about a time you needed help.

**Answer:** Show that you first attempted to understand the issue yourself, then asked a focused question with the evidence you had collected. Explain what you learned.

---

# 22. Experience-Based Questions

## Q165. Walk me through an ETL project you built.

**Answer structure:**

```text
Source
 ↓
Ingestion
 ↓
Staging
 ↓
Transformation
 ↓
Data Warehouse
 ↓
Reporting
```

Then explain:
- Why you chose each layer.
- Data-quality checks.
- Error handling.
- Logging.
- Incremental strategy.
- Monitoring.
- The hardest problem you solved.

## Q166. Why did you use a staging layer?

**Answer:** It separates raw ingestion from warehouse transformations, provides a controlled place for validation and cleansing, and makes troubleshooting/reprocessing easier.

## Q167. How did you handle bad records?

**Answer:** I would validate records and route invalid ones to an error/rejection table with the pipeline run ID, source record information, and error reason.

## Q168. How did you handle duplicates?

**Answer:** First identify why duplicates occur. Then enforce the intended grain/business key and use deterministic deduplication or merge logic. Do not simply hide duplicates with `DISTINCT`.

## Q169. How would you scale an ETL project from 1 million to 1 billion records?

**Answer:** Use incremental processing, partitioned columnar storage, distributed processing where needed, efficient warehouse modeling, appropriate indexes/partitioning, parallel ingestion, and monitoring of data volume and resource usage.

## Q170. What was the most difficult ETL bug you encountered?

**Answer:** Use a real example and explain:

```text
Symptom
  ↓
Investigation
  ↓
Root Cause
  ↓
Fix
  ↓
Prevention
```

This is much stronger than only saying what the bug was.

---

# 23. English Technical Interview

## Q171. Explain a data warehouse to a non-technical person.

**Answer:**

> “A data warehouse is a central system where a company stores organized business data so people can analyze it and create reports. Instead of checking many operational systems separately, analysts can use the warehouse as a consistent source for reporting.”

## Q172. Explain a data lake.

**Answer:**

> “A data lake is a scalable storage system where a company can keep large amounts of raw and processed data in different formats. The data can later be processed for analytics, machine learning, or other use cases.”

## Q173. Walk me through a pipeline you built.

**Answer template:**

> “The pipeline started with data coming from ____. I first ingested the data into ____. Then I validated and transformed it by ____. After that, I loaded it into ____, where it was consumed by ____. I also added logging and error handling. The most difficult issue was ____, and I solved it by ____.”

## Q174. What happens when your pipeline fails?

**Answer:**

> “First, I would identify the failed stage and inspect the logs. If the failure is transient, I would retry it. If the failure is caused by bad data or a code issue, I would fix the root cause. I would also make sure the pipeline is idempotent so that retrying does not create duplicate data.”

## Q175. What do you say if you don't know the answer?

**Answer:**

> “I don't know the answer yet, but I would start by checking ____. Based on the result, I would investigate ____.”

This is better than confidently guessing.

---

# 24. Recruiter / HR Questions

## Q176. Tell me about yourself.

**Answer template:**

> “I'm a Computer Science graduate with experience working with data analysis and BI, and I've been moving toward data engineering. In my previous role, I worked with operational data, built reports and dashboards, and worked on improving data processes. More recently, I've been focusing on SQL, Python, ETL, data modeling, Spark, and orchestration. I'm now looking for a junior data engineering role where I can work on production data pipelines and continue developing my engineering skills.”

## Q177. Why do you want to become a Data Engineer?

**Answer:**

> “I enjoy working with data, but I'm particularly interested in the engineering side: building reliable pipelines, transforming data, designing storage and models, and making data available for analytics and other systems. I want to move from mainly consuming data to building the infrastructure that makes reliable data possible.”

## Q178. Why this company?

**Answer structure:**

> “I'm interested in the company because of ____. I noticed that your team works with ____, which matches the areas I'm currently developing. I also like the opportunity to work on ____ and learn from an experienced engineering team.”

## Q179. What are your salary expectations?

**Answer:**

> “Based on the responsibilities of the role and the market for junior data engineering positions, I'm looking for a reasonable range, but I'm flexible depending on the overall compensation, responsibilities, and growth opportunities.”

Use a researched local range rather than memorizing a universal number.

## Q180. What is one weakness you're working on?

**Answer:** Pick a genuine but manageable weakness and explain the concrete system you are using to improve it.

Example:

> “Earlier, I sometimes spent too much time trying to solve a problem independently. I've been improving by setting a time limit for investigation and then asking a focused question with the evidence I've collected.”

---

# 25. Questions to Ask the Interviewer

## Q181. What does the current data stack look like?

**Why ask:** Helps you understand the actual technologies used in production.

## Q182. What are the biggest data engineering challenges the team is currently facing?

**Why ask:** Shows that you care about real problems rather than only technologies.

## Q183. What would success look like for someone in this role during the first six months?

**Why ask:** Shows ownership and helps clarify expectations.

## Q184. How does onboarding work for junior engineers?

**Why ask:** Helps you understand mentorship and learning opportunities.

## Q185. What are the main consumers of the data?

**Answer:** This can reveal whether the platform mainly serves analysts, BI, applications, data scientists, or ML systems.

## Q186. Are there any major migrations or architectural changes planned?

**Why ask:** Reveals the direction of the data platform and potential opportunities to learn.

---

# Final Interview Checklist

Before a Junior Data Engineer interview, make sure you can explain and demonstrate:

## SQL
- [ ] JOINs
- [ ] GROUP BY / HAVING
- [ ] CTEs
- [ ] Window functions
- [ ] Deduplication
- [ ] Incremental queries
- [ ] Basic query optimization
- [ ] Indexes
- [ ] NULL handling

## Python
- [ ] Lists / sets / dictionaries
- [ ] `defaultdict`
- [ ] File parsing
- [ ] CSV / JSON
- [ ] Dates and timezones
- [ ] Exceptions
- [ ] Logging
- [ ] Database loading
- [ ] Chunked processing
- [ ] Idempotent scripts

## Data Engineering
- [ ] ETL vs ELT
- [ ] Batch vs streaming
- [ ] Warehouse vs lake
- [ ] Lakehouse
- [ ] Data quality
- [ ] Idempotency
- [ ] Incremental loading
- [ ] CDC
- [ ] Monitoring

## Data Modeling
- [ ] Fact / dimension
- [ ] Star schema
- [ ] Grain
- [ ] Surrogate keys
- [ ] SCD Type 1
- [ ] SCD Type 2

## Spark
- [ ] DataFrame
- [ ] RDD
- [ ] Transformations
- [ ] Actions
- [ ] Lazy evaluation
- [ ] Partitions
- [ ] Shuffle
- [ ] Data skew
- [ ] Broadcast join

## Airflow
- [ ] DAG
- [ ] Tasks
- [ ] Operators
- [ ] Dependencies
- [ ] Retries
- [ ] Backfills
- [ ] Monitoring

## Cloud
- [ ] Azure Data Factory
- [ ] ADLS
- [ ] Databricks
- [ ] Synapse/Fabric concepts
- [ ] S3
- [ ] Glue
- [ ] Athena
- [ ] Redshift

## Behavioral
- [ ] Data-quality story
- [ ] Failure story
- [ ] Learning-new-tool story
- [ ] Conflict story
- [ ] Deadline story
- [ ] Mistake story
- [ ] Asking-for-help story
- [ ] Technical-problem story

---

# Recommended Interview Practice Order

If time is limited, study in this order:

1. **SQL**
2. **Python**
3. **ETL / Pipelines**
4. **Data Quality + Idempotency**
5. **Data Modeling**
6. **Debugging Scenarios**
7. **Spark**
8. **Airflow**
9. **Cloud**
10. **Behavioral + English**

The goal is not to memorize answers. For technical questions, practice explaining **why** you would choose an approach, what can go wrong, and how you would verify that your solution works.
