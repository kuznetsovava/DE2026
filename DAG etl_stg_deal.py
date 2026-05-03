from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import hashlib
from psycopg2.extras import execute_values

default_args = {
    'owner': 'kuznetsova',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

CITY_MAP = {
    'MSK': 56,
    'SPB': 324
}

def get_last_watermark(conn_target, log_table_name):
    with conn_target.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(MAX(load_start), '1900-01-01'::timestamptz)
            FROM meta.load_log
            WHERE status = 'success'
              AND load_start >= current_date - 7
              AND table_name = '{log_table_name}'
        """)
        return cur.fetchone()[0]

def update_load_log(conn_target, table_name, load_start, rows_loaded, status):
    with conn_target.cursor() as cur:
        cur.execute("""
            INSERT INTO meta.load_log (table_name, load_start, load_end, rows_loaded, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (table_name, load_start, datetime.now(), rows_loaded, status))
        conn_target.commit()

def load_deal_staging(city_ccode, conn_id, **context):
    target_hook = PostgresHook(postgres_conn_id='DE2026_conn')
    source_hook = PostgresHook(postgres_conn_id=conn_id)
    log_table = f'stg.deal.{city_ccode}'

    with target_hook.get_conn() as conn_target, source_hook.get_conn() as conn_source:
        try:
            last_ts = get_last_watermark(conn_target, log_table)
            logging.info(f"[{city_ccode}] Last successful load: {last_ts}")

            with conn_source.cursor() as src_cur:
                src_cur.execute("""
                    SELECT id, datetime, amount::numeric(15,2), status, plan_service_date
                    FROM public.deal
                    WHERE status <> 1
                      AND datetime > %s - interval '10 days'
                """, (last_ts,))
                rows = src_cur.fetchall()

            if not rows:
                logging.info(f"[{city_ccode}] No new deals")
                update_load_log(conn_target, log_table, datetime.now(), 0, 'success')
                return

            new_rows = []
            for deal_id, dt, amount, status, plan_dt in rows:
                raw_key = f"{deal_id}_{city_ccode}"
                hash_key = hashlib.md5(raw_key.encode('utf-8')).hexdigest()
                new_rows.append((deal_id, dt, amount, status, plan_dt, city_ccode, hash_key))

            with conn_target.cursor() as tgt_cur:
                tgt_cur.execute("DELETE FROM stg.deal WHERE city_ccode = %s", (city_ccode,))
                execute_values(
                    tgt_cur,
                    """INSERT INTO stg.deal
                       (id, datetime, amount, status, plan_service_date, city_ccode, hash_key, loaded_at)
                       VALUES %s""",
                    new_rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"
                )
                conn_target.commit()

            update_load_log(conn_target, log_table, datetime.now(), len(new_rows), 'success')
            logging.info(f"[{city_ccode}] Staging loaded {len(new_rows)} rows")

        except Exception as e:
            conn_target.rollback()
            update_load_log(conn_target, log_table, datetime.now(), 0, 'failed')
            logging.error(f"[{city_ccode}] Error: {e}")
            raise

def load_fact_deal(**context):
    target_hook = PostgresHook(postgres_conn_id='DE2026_conn')

    with target_hook.get_conn() as conn_target:
        try:
            with conn_target.cursor() as cur:
                # Добавлен hash_key – он уже готов, пересчитывать не нужно
                cur.execute("""
                    SELECT id, datetime, amount, plan_service_date, city_ccode, hash_key
                    FROM stg.deal
                    WHERE city_ccode IN ('MSK', 'SPB')
                """)
                rows = cur.fetchall()

            if not rows:
                logging.info("No data in stg.deal for fact load")
                return

            fact_rows = []
            skipped = 0
            for deal_id, dt, amount, plan_dt, city_ccode, hash_key in rows:
                city_id = CITY_MAP.get(city_ccode)
                if city_id is None:
                    logging.warning(f"Unknown city_ccode '{city_ccode}' for deal {deal_id}, skipped")
                    skipped += 1
                    continue

                date_id = int(dt.strftime('%Y%m%d'))
                plan_date_id = int(plan_dt.strftime('%Y%m%d')) if plan_dt else None

                # hash_key используется из staging, без изменений
                fact_rows.append((hash_key, deal_id, date_id, city_id, plan_date_id, amount))

            if not fact_rows:
                logging.info("No valid fact rows (all skipped)")
                return

            with conn_target.cursor() as cur:
                execute_values(
                    cur,
                    """INSERT INTO dwh.fact_deal
                       (hash_key, deal_id, date_id, city_id, plan_date_id, amount)
                       VALUES %s
                       ON CONFLICT (hash_key) DO NOTHING""",
                    fact_rows,
                    template="(%s, %s::int, %s::int, %s::int, %s::int, %s::numeric(15,2))"
                )
                conn_target.commit()
                logging.info(f"Fact loaded {cur.rowcount} rows, skipped={skipped}")

            update_load_log(conn_target, 'dwh.fact_deal', datetime.now(), len(fact_rows), 'success')

        except Exception as e:
            conn_target.rollback()
            update_load_log(conn_target, 'dwh.fact_deal', datetime.now(), 0, 'failed')
            logging.error(f"Error in load_fact_deal: {e}")
            raise

with DAG(
    dag_id='dag_deal_to_fact_deal',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['stg', 'de2026', 'deal']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    load_msk = PythonOperator(
        task_id='load_deal_staging_msk',
        python_callable=load_deal_staging,
        op_kwargs={'city_ccode': 'MSK', 'conn_id': 'CRM_MscDB_conn'}
    )
    load_spb = PythonOperator(
        task_id='load_deal_staging_spb',
        python_callable=load_deal_staging,
        op_kwargs={'city_ccode': 'SPB', 'conn_id': 'CRM_SPbDB_conn'}
    )

    load_fact = PythonOperator(
        task_id='load_fact_deal',
        python_callable=load_fact_deal
    )

    start >> [load_msk, load_spb] >> load_fact >> end