# Evaluating Large Language Models for Data Pipeline Generation  

## Authors  
Chiara Rucco, Motaz Saad, Tobia Martina, Antonella Longo  

## Overview  
This repository contains the code and workflows used to evaluate the capabilities of Large Language Models (LLMs) in generating data pipelines using **Apache Airflow** and **Databricks**. The experiments assess the accuracy, reliability, and limitations of LLM-generated pipelines by comparing multiple models across different scenarios.  

The results and insights from these evaluations are presented in our paper:  
**_"Evaluating Large Language Models for Data Pipeline Generation."_**  

## Repository Structure  
This repository includes:  

- **`airflow/`** – Contains an Apache Airflow setup, including a virtual environment, DAGs, and configurations.  
- **`databricks/`** – Contains Databricks notebooks generated during two experiments, comparing LLM-generated pipelines with manually optimized versions.  



## Key Findings  
The study highlights several factors that impact LLM-generated pipeline effectiveness:  
- **Prompt Engineering**: The quality and structure of prompts significantly influence pipeline generation accuracy.  
- **Platform Limitations**: Code-based platforms like Databricks allow greater automation, while platforms like Azure Data Factory require manual configuration.  
- **Error Handling**: Some LLMs generate incomplete or incorrect code, necessitating human intervention.  
- **Scalability**: Manually optimized pipelines tend to perform better in complex scenarios.  


