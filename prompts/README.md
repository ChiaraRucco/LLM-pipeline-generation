# Task Prompts

This directory documents the exact prompts used for the ICDE 2027 submission
"_Empirical evaluation of large language models for data pipeline generation across
code-based, low-code, and hybrid platforms_". The same prompts are reproduced in
`openrouter_multirun/prompts.json` and are used by the multi-run validation script.

## Tasks

| Task | Name | Platforms |
|------|------|-----------|
| T1 | Basic API pipeline | Airflow, ADF, Databricks |
| T2 | ETL pipeline | Airflow, ADF, Databricks |
| T3 | Merge | Databricks |
| T4 | Filter | Databricks |
| T5 | Join | Databricks |

## Prompt templates

### T1 - Basic API pipeline

- Airflow: "Generate an Airflow DAG that: Create a data pipeline that calls
  https://api.spacexdata.com/v3/launches/latest, extracts the response, and posts it to
  https://httpbin.org/post. The steps must run in order, the pipeline must retry on failure
  with a timeout, and the API response must be handled securely."
- ADF: "Write JSON code to create an Azure Data Factory pipeline: Create a data pipeline
  that calls https://api.spacexdata.com/v3/launches/latest, extracts the response, and posts
  it to https://httpbin.org/post. The steps must run in order, the pipeline must retry on
  failure with a timeout, and the API response must be handled securely."
- Databricks: "Generate a Databricks notebook that: Create a data pipeline that calls
  https://api.spacexdata.com/v3/launches/latest, extracts the response, and posts it to
  https://httpbin.org/post. The steps must run in order, the pipeline must retry on failure
  with a timeout, and the API response must be handled securely."

### T2 - ETL pipeline

"Generate an ETL pipeline for {platform} that retrieves data from an API
(.../launches/latest), performs a basic transformation, and posts the result to another API
(httpbin.org/post); include print statements for debugging and display the execution time of
the entire process."

### T3 - Merge

"Get data from two endpoints (.../launches, .../rocket); combine each launch record with the
name of the corresponding rocket; send the final result to httpbin.org/post. The script must
provide status updates, report any errors encountered, confirm the outcome of the final
data-sending step, and measure/report execution times."

### T4 - Filter

"Get launch data from .../launches; filter the list based on launch year and launch success
status; send the chosen records to httpbin.org/post. The script must provide status updates,
report any errors encountered, confirm the outcome of the final data-sending step, and
measure/report execution times."

### T5 - Join

"Get data from two endpoints (.../launches, .../rocket); perform a join between the launches
and the rockets; send the chosen records to httpbin.org/post. The script must provide status
updates, report any errors encountered, confirm the outcome of the final data-sending step,
and measure/report execution times."

## Models and versions

| Model | Version (paper) | Notes |
|-------|-----------------|-------|
| GPT-4o | gpt-4o-2024-08-06 | Model snapshot pinned as of the access window |
| Claude 3.7 Sonnet | claude-3-7-sonnet-20250219 | |
| Qwen 2.5-Max | qwen-max-2025-01-25 | |
| DeepSeek-V3 | deepseek-chat (V3-0324) | Served from 2025-03-24 |
| Gemini 2.0 Flash | gemini-2.0-flash-001 | GA 2025-02-05 |
| Llama 3.3 70B | llama-3.3 (2024 release) | |

Experiments for the ICDE submission were conducted between March and June 2025.

## Validation checks

| Task | Keywords checked in generated code/output |
|------|------------------------------------------|
| T1 | api.spacexdata.com, httpbin.org/post, retry, timeout |
| T2 | launches, httpbin.org/post, print, execution time |
| T3 | launches, rocket, httpbin.org/post, rocket name |
| T4 | launches, filter, launch year, success, httpbin.org/post |
| T5 | launches, rocket, join, httpbin.org/post |