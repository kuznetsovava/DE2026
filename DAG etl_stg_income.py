from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import hashlib
from dateutil import parser
from psycopg2.extras import execute_values

default_args = {
    'owner': 'kuznetsova',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_last_watermark(conn_target, table_name):
    """
    Возвращает MAX(load_start) последней успешной загрузки таблицы.
    Если записей нет – 1900-01-01 (заберём все данные).
    """
    with conn_target.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(MAX(load_start), '1900-01-01'::timestamptz)
            FROM meta.load_log
            WHERE status='success' 
              AND load_start >= current_date - 7
              AND table_name='{table_name}'
        """)
        return cur.fetchone()[0]   # timestamptz

def update_load_log(conn_target, table_name, load_start, rows_loaded, status):
    with conn_target.cursor() as cur:
        cur.execute("""
            INSERT INTO meta.load_log (table_name, load_start, load_end, rows_loaded, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (table_name, load_start, datetime.now(), rows_loaded, status))
        conn_target.commit()

def parse_date_string(date_str):
    """Строку -> date (или None)"""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        dt = parser.parse(date_str)
        return dt.date()
    except Exception:
        logging.warning(f"Could not parse date string: {date_str}")
        return None


# --------------------------------- stg ----------------------------------
def load_income(**context):
    target_hook = PostgresHook(postgres_conn_id='DE2026_conn')
    source_hook = PostgresHook(postgres_conn_id='1c_conn')

    with target_hook.get_conn() as conn_target, source_hook.get_conn() as conn_source:
        try:
            # 1. Дата последней загрузки (load_start)
            last_ts = get_last_watermark(conn_target, 'stg.income')
            last_date = last_ts.date()   # нужна только дата
            logging.info(f"Loading income with date > {last_date}")

            # 2. Забираем ВСЕ строки-сырцы (date – varchar)
            with conn_source.cursor() as src_cur:
                src_cur.execute("""
                    SELECT id, abs(amount::numeric), tauschpartner, city, deal_id, date
                    FROM reporting.income
                """)
                raw_rows = src_cur.fetchall()

            # 3. Парсим дату и отбираем новые строки
            new_rows = []   # элементы: (id, amount, partner, city, deal_id, date_str, ddate)
            for row in raw_rows:
                id_, amount, partner, city, deal_id, date_str = row
                ddate = parse_date_string(date_str)
                if ddate is None:
                    logging.warning(f"Row id={id_} skipped, unparseable date: {date_str}")
                    continue
                # Инкремент: только даты > даты последней загрузки
                if ddate > last_date:
                    new_rows.append((id_, amount, partner, city, deal_id, date_str, ddate))

            if not new_rows:
                logging.info("No new rows to load into stg.income")
                update_load_log(conn_target, 'stg.income', datetime.now(), 0, 'success')
                return

            # 4. Полная перезапись staging (TRUNCATE + INSERT)
            with conn_target.cursor() as tgt_cur:
                tgt_cur.execute("TRUNCATE TABLE stg.income")
                execute_values(
                    tgt_cur,
                    """INSERT INTO stg.income
                       (id, amount, tauschpartner, city, deal_id, date, ddate, loaded_at)
                       VALUES %s""",
                    new_rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"
                )
                conn_target.commit()

            load_start = datetime.now()
            update_load_log(conn_target, 'stg.income', load_start, len(new_rows), 'success')
            logging.info(f"Loaded {len(new_rows)} rows into stg.income, waterline date > {last_date}")

        except Exception as e:
            conn_target.rollback()
            update_load_log(conn_target, 'stg.income', datetime.now(), 0, 'failed')
            logging.error(f"Error in load_income: {e}")
            raise


# --------------------------------- dwh ---------------------------------
def load_fact_cashflow(**context):
    target_hook = PostgresHook(postgres_conn_id='DE2026_conn')

    with target_hook.get_conn() as conn_target:
        try:
            with conn_target.cursor() as cur:
                cur.execute("""
                    SELECT id, amount, city, deal_id, ddate
                    FROM stg.income
                    WHERE ddate IS NOT NULL
                """)
                rows = cur.fetchall()

            if not rows:
                logging.info("No data in stg.income, skipping fact load")
                return

            fact_rows = []
            for cashflow_id, amount, city, deal_id, dt in rows:
                operation_id = 1
                date_id = int(dt.strftime('%Y%m%d'))
                unique_str = f"{cashflow_id}_{operation_id}_{date_id}_{city}"
                hash_key = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
                fact_rows.append((hash_key, cashflow_id, operation_id, city, deal_id, date_id, amount))

            with conn_target.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO dwh.fact_cashflow
                       (hash_key, cashflow_id, operation_id, city_id, deal_id, date_id, amount)
                       VALUES %s
                       ON CONFLICT (hash_key) DO NOTHING""",
                    fact_rows,
                    template="(%s, %s, %s, %s, %s, %s, %s)"
                )
                conn_target.commit()
                logging.info(f"Inserted {cur.rowcount} new rows into dwh.fact_cashflow")

            load_start = datetime.now()
            update_load_log(conn_target, 'dwh.fact_cashflow', load_start, len(fact_rows), 'success')

        except Exception as e:
            conn_target.rollback()
            update_load_log(conn_target, 'dwh.fact_cashflow', datetime.now(), 0, 'failed')
            logging.error(f"Error in load_fact_cashflow: {e}")
            raise


# --------------------------------- DAG ---------------------------------
with DAG(
    dag_id='dag_income_to_cashflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['stg', 'de2026', 'income']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    load_stg = PythonOperator(
        task_id='full_refresh_income_stg',
        python_callable=load_income,
    )
    load_fact = PythonOperator(
        task_id='load_fact_cashflow_incremental',
        python_callable=load_fact_cashflow,
    )

    start >> load_stg >> load_fact >> end