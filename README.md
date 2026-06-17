# Apache Airflow Practice Lab ⚙️

Hands-on practice with Apache Airflow for pipeline orchestration — 
covering DAG design, scheduling, task dependencies, and operators.

## 🎯 Purpose

Building real-world Airflow skills as part of an active upskilling 
roadmap toward the modern data engineering stack (Airflow + dbt + 
PostgreSQL + PySpark + Databricks).

## 🛠️ Tech Stack

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

## 📁 Structure

Airflow_Prac/

└── dags/           # DAG definitions and pipeline logic

## 📚 Topics Covered

- DAG structure and best practices
- Task dependencies and execution order  
- Scheduling with cron expressions
- PythonOperator, BashOperator, and beyond
- XComs for task communication
- Connection and variable management

## 🚀 How to Run Locally

```bash
# Install Airflow
pip install apache-airflow

# Initialise the database
airflow db init

# Start the web server (default port 8080)
airflow webserver --port 8080

# In a separate terminal, start the scheduler
airflow scheduler
```

Then place DAG files in your `~/airflow/dags/` folder and they 
will appear in the Airflow UI.

## 🗺️ Learning Roadmap

- [x] Airflow setup and configuration
- [x] Basic DAG creation and scheduling
- [ ] Dynamic DAGs and task groups
- [ ] Airflow with Docker Compose
- [ ] Integration with PostgreSQL and GCP

## 🔗 Related Projects

- [`dbt_postgres_labs`](https://github.com/garu-farcis/dbt_postgres_labs) — dbt + PostgreSQL transformation layer
- [`Python-Practice`](https://github.com/garu-farcis/Python-Practice) — Python fundamentals for data engineering
