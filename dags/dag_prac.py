"""15 Difficult Apache Airflow Interview Questions for Data Engineering (3+ YOE)

1. **Dynamic Task Mapping at Scale**
   Scenario: You need to process thousands of daily partitions/files where the list is only known at runtime.
   Problem: Design a DAG that dynamically maps tasks using TaskFlow API ( `@task.map` ) over a list pulled from S3 / database. Handle mapping limits and failure isolation.
   Sample: `files = ["2026-07-01.parquet", ..., "2026-07-31.parquet"]` → map a processing task per file.

2. **Complex Cross-DAG Dependencies with TriggerDagRunOperator**
   Scenario: Microservices architecture where DAG A (ingestion) must trigger DAG B (processing) with config, and wait for completion.
   Problem: Implement bidirectional dependency with `TriggerDagRunOperator` + ExternalTaskSensor, passing parameters via conf and handling backfill scenarios.

3. **Idempotent & Re-runnable ETL Pipeline**
   Scenario: Daily ETL job that can fail midway and must be safely re-run without data duplication.
   Problem: Design pattern using `ShortCircuitOperator`, `BranchPythonOperator`, and upsert logic with proper task dependencies and `depends_on_past`.

4. **Advanced Sensor Patterns with Custom Poke**
   Scenario: Waiting for external system (Kafka topic lag = 0 or S3 prefix completeness).
   Problem: Create a custom Sensor that uses exponential backoff, soft_fail, and timeout. Compare with `ExternalTaskSensor` and `FileSensor`.

5. **TaskFlow API vs Traditional PythonOperator (Performance & Readability)**
   Scenario: Complex data pipeline with heavy XCom usage (DataFrames > 100MB).
   Problem: Show when to use TaskFlow `@task` with `multiple_outputs` vs classic operators, and how to avoid XCom bloat using S3 intermediate storage.

6. **Error Handling & Retry Strategy at Scale**
   Scenario: Production DAG with 500+ tasks where some are flaky (API calls) and some are critical.
   Problem: Design a pattern using `on_failure_callback`, custom `retry_exponential_backoff`, `SLA`, and `trigger_rule='ALL_DONE'` for cleanup tasks.

7. **Backfill Safety with LatestOnlyOperator & Time-based Branching**
   Scenario: Prevent accidental backfills from overwriting production data.
   Problem: Implement a DAG that skips execution for historical runs using `LatestOnlyOperator` or custom Python logic based on `data_interval_start`.

8. **DAG Factory Pattern for 100+ Similar Pipelines**
   Scenario: You manage 150 similar tables with different schedules and dependencies.
   Problem: Design a DAG factory function that reads config from YAML/ database and generates DAGs dynamically with proper unique task_ids.

9. **Resource Pool & Concurrency Control**
   Scenario: Heavy Spark jobs that can overload the cluster if run in parallel.
   Problem: Configure and use `Pool` with `slots`, set task-level `pool_slots`, and integrate with Celery/Kubernetes Executor.

10. **KubernetesPodOperator Best Practices**
    Scenario: Running Spark / Python jobs in isolated pods.
    Problem: Design a DAG using `KubernetesPodOperator` with proper image versioning, volume mounts, env_vars from Secrets, resource requests/limits, and XCom push via sidecar.

11. **XCom Backend with S3 / Redis for Large Payloads**
    Scenario: Passing large metadata or small DataFrames between tasks.
    Problem: Configure custom XCom backend (S3XComBackend) and demonstrate usage with `xcom_push` / `xcom_pull` for DataFrame references instead of raw data.

12. **Airflow + dbt Integration Patterns**
    Scenario: Running dbt models as part of a larger orchestration.
    Problem: Compare `BashOperator`/`PythonOperator` vs `DbtCloudRunJobOperator` / `DbtOperator`, including how to handle model selection and tests.

13. **Event-Driven DAGs with Deferrable Operators**
    Scenario: Trigger processing only when new files arrive (cost optimization).
    Problem: Implement using `Deferrable` sensors (Async) with `Trigger` + `PythonSensor` or `ExternalEventSensor` (Airflow 2.4+).

14. **Monitoring, Logging & Alerting Strategy**
    Scenario: Production environment with hundreds of DAGs.
    Problem: Design a strategy using StatsD metrics, custom callbacks for Slack/Email on failure, `airflow dags test`, and integration with Prometheus + Grafana.

15. **Zero-Downtime DAG Upgrade & Versioning**
    Scenario: You need to update a critical daily DAG without breaking ongoing runs or backfills.
    Problem: Explain DAG versioning strategies, `catchup=False`, `max_active_runs`, and using different DAG IDs during transition."""


"""1. **Dynamic Task Mapping at Scale**
   Scenario: You need to process thousands of daily partitions/files where the list is only known at runtime.
   Problem: Design a DAG that dynamically maps tasks using TaskFlow API ( `@task.map` ) over a list pulled from S3 / database. Handle mapping limits and failure isolation.
   Sample: `files = ["2026-07-01.parquet", ..., "2026-07-31.parquet"]` → map a processing task per file."""

