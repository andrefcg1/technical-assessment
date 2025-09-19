from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess, os, shutil

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbt_bigquery_pipeline",
    default_args=default_args,
    description="dbt pipeline for BigQuery data processing",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["dbt", "bigquery", "data-pipeline"],
)

DBT_DIR = "/home/airflow/gcs/dags/dbt"
KEYFILE = "/home/airflow/gcs/dags/dbt/validation-account.json"  # corrigido

def _resolve_dbt_cmd():
    # 1) tente python -m dbt (preferido)
    r = subprocess.run(["python", "-c", "import dbt,sys; print(getattr(dbt,'__version__','?')); import importlib; print(importlib.util.find_spec('dbt.__main__') is not None)"],
                       text=True, capture_output=True)
    print("dbt import check ->", r.stdout.strip() or r.stderr)
    has_main = "True" in r.stdout
    if has_main:
        return ["python", "-m", "dbt"]
    # 2) fallback: binário dbt no PATH
    dbt_bin = shutil.which("dbt")
    if dbt_bin:
        print("Using dbt binary at:", dbt_bin)
        return [dbt_bin]
    raise RuntimeError("dbt não encontrado/instalado corretamente no ambiente do Composer.")

def run_dbt(args: list[str]):
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_DIR
    env["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE

    subprocess.run(["which", "python"], check=False)
    cmd = _resolve_dbt_cmd() + args + ["--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    print("About to run:", " ".join(cmd))

    result = subprocess.run(cmd, cwd=DBT_DIR, env=env, text=True, capture_output=True)
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} failed with {result.returncode}")
    return result.stdout

def dbt_debug_task():    return run_dbt(["debug",   "--target", "bigquery"])
def dbt_deps_task():     return run_dbt(["deps",    "--target", "bigquery"])
def dbt_seed_task():     return run_dbt(["seed",    "--target", "bigquery", "--full-refresh"])
def dbt_snapshot_task(): return run_dbt(["snapshot","--target", "bigquery"])
def dbt_run_task():      return run_dbt(["run",     "--target", "bigquery"])
def dbt_test_task():     return run_dbt(["test",    "--target", "bigquery"])

dbt_debug = PythonOperator(task_id="dbt_debug",    python_callable=dbt_debug_task,    dag=dag)
dbt_deps  = PythonOperator(task_id="dbt_deps",     python_callable=dbt_deps_task,     dag=dag)
dbt_seed  = PythonOperator(task_id="dbt_seed",     python_callable=dbt_seed_task,     dag=dag)
dbt_snap  = PythonOperator(task_id="dbt_snapshot", python_callable=dbt_snapshot_task, dag=dag)
dbt_run   = PythonOperator(task_id="dbt_run",      python_callable=dbt_run_task,      dag=dag)
dbt_test  = PythonOperator(task_id="dbt_test",     python_callable=dbt_test_task,     dag=dag)

dbt_debug >> dbt_deps >> dbt_seed >> dbt_snap >> dbt_run >> dbt_test