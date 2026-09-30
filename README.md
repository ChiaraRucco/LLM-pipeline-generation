# Empirical Evaluation of Large Language Models for Data Pipeline Generation Across Code-Based, Low-Code, and Hybrid Platforms

## Authors
Chiara Rucco, Motaz Saad, Tobia Martina, Antonella Longo

## Overview
This repository contains the artifacts used to evaluate the capabilities of Large Language
Models (LLMs) in generating data pipelines using **Apache Airflow**, **Azure Data Factory**,
and **Databricks**. The experiments assess the accuracy, reliability, and limitations of
LLM-generated pipelines by comparing multiple models across different scenarios and
platforms.

The results and insights from these evaluations are presented in our paper:
**_"Empirical Evaluation of Large Language Models for Data Pipeline Generation Across
Code-Based, Low-Code, and Hybrid Platforms."_** This repository is provided as the artifact
for the ICDE 2027 Experiment, Analysis and Benchmark track.

## Repository Structure
- **`airflow/`** – Apache Airflow setup, including a virtual environment, DAGs, and
  configurations (code-based platform; T1 and T2 tasks).
- **`databricks/`** – Databricks notebooks generated for all five tasks (code-based /
  notebook platform).
- **`prompts/`** – The exact task prompts, model versions, and validation checks used in the
  experiments (see [`prompts/README.md`](prompts/README.md)).

> Note: Azure Data Factory (low-code platform) pipelines and the associated run logs are
> being added; task prompts for ADF are already documented in `prompts/`.

## Key Findings
The study highlights several factors that impact LLM-generated pipeline effectiveness:
- **Prompt Engineering**: The quality and structure of prompts significantly influence
  pipeline generation accuracy.
- **Platform Limitations**: Code-based platforms like Databricks allow greater automation,
  while low-code platforms like Azure Data Factory require manual configuration.
- **Error Handling**: Some LLMs generate incomplete or incorrect code, necessitating human
  intervention.
- **Scalability**: Manually optimized pipelines tend to perform better in complex scenarios.

## Citing this work
If you use these artifacts, please cite the ICDE 2027 submission by Saad, Rucco, Martina,
and Longo, and our earlier study in *Future Generation Computer Systems* (FGCS 183:108587).