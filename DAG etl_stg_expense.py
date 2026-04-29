from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'kuznetsova',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_last_watermark(conn_target, table_name):
    with conn_target.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(MAX(datetime), '1900-01-01'::timestamptz)
            FROM {table_name}
        """)
        return cur.fetchone()[0]

def update_load_log(conn_target, table_name, load_start, rows_loaded, status):
    with conn_target.cursor() as cur:
        cur.execute("""
            INSERT INTO meta.load_log (table_name, load_start, load_end, rows_loaded, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (table_name, load_start, datetime.now(), rows_loaded, status))
        conn_target.commit()

def load_expense(**context):
    target_hook = PostgresHook(postgres_conn_id='DE2026_conn')
    source_hook = PostgresHook(postgres_conn_id='1c_conn')
    
    conn_target = target_hook.get_conn()
    conn_source = source_hook.get_conn()
    
    try:
        last_ts = get_last_watermark(conn_target, 'stg.expense')
        logging.info(f"Loading expense from {last_ts}")
        with conn_source.cursor() as cur_src:
            cur_src.execute("""
                SELECT id, datetime, abs(amount::numeric), category, CURRENT_TIMESTAMP as loaded_at
                FROM reporting.expense
                WHERE datetime > %s
            """, (last_ts,))
            rows = cur_src.fetchall()
        
        if rows:
            with conn_target.cursor() as cur_tgt:
                from psycopg2.extras import execute_values
                execute_values(cur_tgt, """
                    INSERT INTO stg.expense (id, datetime, amount, category, loaded_at)
                    VALUES %s
                """, rows)
                conn_target.commit()
        
        load_start = datetime.now()
        update_load_log(conn_target, 'stg.expense', load_start, len(rows), 'success')
        logging.info(f"Loaded {len(rows)} rows into stg.expense")
        
    except Exception as e:
        conn_target.rollback()
        update_load_log(conn_target, 'stg.expense', datetime.now(), 0, 'failed')
        logging.error(f"Error in load_expense: {e}")
        raise
    finally:
        conn_target.close()
        conn_source.close()

with DAG(
    dag_id='etl_stg_expense',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['stg', 'de2026', 'expense']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    task_load = PythonOperator(
        task_id='load_expense',
        python_callable=load_expense,
    )
    start >> task_load >> end