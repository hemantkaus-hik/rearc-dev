# Process Documentation

## Overview

This project implements a medallion-style data pipeline in Databricks using **Bronze, Silver, and Gold** layers. The solution was implemented in two parallel ways: **PySpark notebooks** and **Databricks SQL notebooks**. This demonstrates the same ingestion and transformation requirements using both programmatic Spark APIs and SQL, while keeping the underlying architecture consistent.

The pipeline is designed so that ingestion can be re-run safely. Bronze preserves source-oriented data and ingestion metadata, Silver applies cleansing, typing, deduplication, and incremental merge logic, and Gold is intended for business-ready aggregations and analytical measures.

## 1. Architecture

### Bronze layer

The Bronze layer is the raw ingestion boundary. Source files are loaded with minimal transformation so that the original source representation remains available for troubleshooting and reprocessing.

For delimited/tabular files, the ingestion applies only lightweight standardization such as defined schemas, trimming string columns, converting empty strings to NULL, and retaining source metadata where applicable.

For the JSON population source, the JSON is retained in Bronze as `raw_json` rather than prematurely flattening the complete document. This keeps Bronze close to the source and allows Silver to own the parsing and normalization logic.

This separation is intentional: if transformation rules change later, the Silver layer can be rebuilt from Bronze without having to retrieve the files again.


table_name ( %_sql_% are from SQL style Notebook while others are from Pyspark)
bronze_pr_population
bronze_sql_pr_footnote
bronze_pr_class
bronze_pr_measure
bronze_pr_series
bronze_pr_duration
bronze_sql_pr_class
bronze_sql_pr_data
bronze_sql_pr_seasonal
bronze_sql_pr_duration
bronze_sql_pr_sector
bronze_pr_seasonal
bronze_sql_pr_series
bronze_sql_pr_population
bronze_pr_sector
bronze_sql_pr_measure
bronze_sql_pr_period
bronze_pr_footnote
bronze_pr_data
bronze_pr_period

### Silver layer

Silver represents validated and standardized records suitable for downstream processing. The main responsibilities are:

- Parse and normalize Bronze data.
- Apply appropriate data types.
- Trim strings and normalize blank values.
- Remove source duplicates.
- Use meaningful business keys to identify existing records.
- Insert new records and update records whose values have changed.

For the population JSON, `from_json` converts `raw_json` into a structured value and `EXPLODE` converts the `data` array into individual rows. Fields such as Nation, Nation ID, Population, and Year are then exposed as typed Silver columns.

Incremental processing is implemented using Delta `MERGE`. For example, `nation_id + year` can identify a population record. If the key is new, the row is inserted. If the key already exists and tracked values have changed, the row is updated. If the incoming row is unchanged, no update is required. This makes repeated execution idempotent from the perspective of current-state Silver data and prevents simple reruns from continuously appending duplicate records.

NULL-safe comparison is preferred where change detection must also recognize transitions between NULL and non-NULL values.

table_name ( %_sql_% are from SQL style Notebook while others are from Pyspark)
silver_pr_class
silver_pr_data
silver_pr_duration
silver_pr_footnote
silver_pr_measure
silver_pr_period
silver_pr_seasonal
silver_pr_sector
silver_pr_series
silver_sql_pr_class
silver_sql_pr_data
silver_sql_pr_duration
silver_sql_pr_footnote
silver_sql_pr_measure
silver_sql_pr_period
silver_sql_pr_population
silver_sql_pr_seasonal
silver_sql_pr_sector
silver_sql_pr_series

### Gold layer

Gold is the consumption layer. It should contain reporting-ready or business-oriented results rather than raw ingestion logic. Measures such as averages, population standard deviation, minimum/maximum values, and other analytical aggregations belong here when required by the use case.

Keeping aggregation in Gold avoids losing useful record-level detail in Silver and makes the layers easier to reuse for multiple analytical requirements.

table_name( %_sql_% are from SQL style Notebook while others are from Pyspark)
gold_sql_pr_population_stats_2013_2018_sql
gold_sql_pr_prs30006032_q01_population_sql
gold_prs30006032_q01_population
gold_bls_best_year_by_series
gold_sql_pr_bls_best_year_by_series_sql
gold_population_stats_2013_2018

### PySpark versus SQL

Both implementations follow the same architectural principles rather than creating two different designs.

**PySpark is the primary choice when transformation logic needs to be dynamic or reusable.** It is particularly useful for iterating through schemas, dynamically cleaning columns, handling multiple files/tables through configuration, and building reusable ingestion functions.

**SQL is valuable for transparency and declarative data transformations.** The SQL implementation makes Bronze-to-Silver parsing, cleansing, deduplication, and Delta `MERGE` logic easy to review. For teams with strong SQL skills, this can also make maintenance straightforward.

For a production implementation, I would avoid maintaining two equivalent pipelines indefinitely because this duplicates testing and maintenance. I would select one primary implementation based on team ownership and operational requirements. For this exercise, implementing both demonstrates that the architecture is independent of the API used to express it.
### For rearc assessment I would like to consider SQL approach Notebook.

### Safe re-runs

Safe reprocessing was a deliberate design goal. The approach is:

1. Preserve source-level data in Bronze.
2. Define deterministic business keys in Silver.
3. Deduplicate incoming records before merging.
4. Use Delta `MERGE` rather than unconditional append for Silver current-state records.
5. Insert keys that do not exist.
6. Update existing keys only when relevant values have actually changed.
7. Leave unchanged records untouched.

This prevents a normal rerun of the same source from producing duplicate Silver records.

## 2. Trade-offs and Production Considerations

The implementation is intentionally appropriate for an exercise/small dataset. For a real client implementation, I would strengthen several areas.

### Schema drift

The current solution uses an expected schema for predictable typing. In production, source schema changes would be explicitly governed. Unexpected columns, missing required fields, incompatible type changes, and malformed records should be detected and surfaced rather than silently accepted.

For file-based Databricks ingestion at scale, Auto Loader and controlled schema evolution would be considered. Schema changes should normally pass validation before they propagate into curated Silver contracts.

### Data volume and performance

For small datasets, scanning the Bronze data and merging the result is acceptable. At larger volumes, I would avoid repeatedly processing the entire historical Bronze dataset.

Production improvements could include:

- Incremental ingestion based on newly arrived files or checkpoints.
- Auto Loader for continuously arriving cloud files where appropriate.
- Partition pruning or Delta data-skipping-friendly layouts based on real access patterns.
- Liquid Clustering for sufficiently large Delta tables when query patterns justify it.
- MERGE source reduction so only changed/new records participate in the operation.

Optimization choices would be based on observed table size, file counts, query patterns, and Spark execution metrics rather than applied automatically.

### Historical changes

The current `MERGE` represents a current-state/Type 1 approach: when a business key exists and an attribute changes, the existing Silver row is updated.

If a client requirement is to retain every business-state change, I would use an SCD Type 2 design with fields such as `effective_from`, `effective_to`, and `is_current`, or another append-oriented history pattern. This distinction should be driven by whether the business needs the latest truth or point-in-time history.

### Data quality

The exercise includes basic cleansing and duplicate handling. A production pipeline should make data-quality rules explicit. Examples for the population dataset could include:

- `nation_id` must not be NULL for records that require it as a key.
- `year` must convert successfully to INT and fall in an acceptable domain.
- `population` should not be negative.
- Duplicate business keys within the same incoming batch should be detected before `MERGE`.
- Corrupt or unparseable records should be quarantined with an error reason and source-file information.

Quality results should be measurable, not just implemented as filters that silently discard data.

### Cost and compute

For production I would size compute based on actual workload, use job/serverless compute where appropriate, terminate idle resources, and monitor execution time and data scanned. Re-reading all historical Bronze data on every run would become increasingly expensive, so incremental ingestion is important for both performance and cost.

### Access control

In a real client environment I would use Unity Catalog privileges to separate responsibilities across Bronze, Silver, and Gold. Raw data access can be more restrictive, while consumers can be granted access only to approved Silver or Gold objects. Service principals should be preferred for automated production jobs, and secrets/tokens should not be embedded in notebook code.

### Monitoring and operations

A production implementation should record operational metrics such as:

- Pipeline/job run ID.
- Source file processed.
- Ingested row count.
- Inserted, updated, rejected, and duplicate counts.
- Start/end timestamps and duration.
- Data-quality failures.
- Exception/error details.

Databricks Workflows/job monitoring and alerting can then be used to make failures operationally visible. Centralized audit/log tables would also make reconciliation and support substantially easier.

## 3. Retrospective

The hardest part was not reading the files themselves. The more difficult design problem was making the pipeline behave correctly when it is executed repeatedly and when source data changes.

Three areas required the most care:

1. **Separating Bronze and Silver responsibilities.** It is tempting to completely transform the source during ingestion. Keeping the JSON as `raw_json` in Bronze and parsing/exploding the `data` array in Silver produced a cleaner separation and retained the original source representation.

2. **Defining incremental behavior.** `DISTINCT` alone only removes duplicates from a result set; it does not provide reliable incremental processing across pipeline runs. A business-key-based Delta `MERGE` was required to differentiate new, changed, and unchanged records.

3. **Handling JSON differently without breaking the common architecture.** Tabular files naturally map to rows and columns, whereas the population JSON contains a nested `data` array. The solution therefore preserves JSON in Bronze and uses `from_json` plus `EXPLODE` in Silver while still applying the same overall Bronze/Silver contract.

Implementing the pipeline in both PySpark and SQL also reinforced an important design point: the processing language should not determine the data architecture. The Bronze/Silver/Gold responsibilities, business keys, data-quality rules, and rerun behavior should remain consistent regardless of whether transformations are implemented with the PySpark DataFrame API or Spark SQL.

## Summary

The resulting design favors reproducibility, clear layer responsibilities, and safe re-execution. Bronze retains source-oriented data, Silver produces clean and incrementally maintained records, and Gold is reserved for analytical outputs. PySpark provides flexibility for dynamic/configuration-driven processing, while SQL provides a concise and reviewable representation of the same transformations. For production use, I would extend the solution with stronger schema governance, incremental ingestion, explicit quality/quarantine handling, historical-state requirements where needed, Unity Catalog controls, and operational monitoring.
