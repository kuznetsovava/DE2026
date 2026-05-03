from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import hashlib

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
            SELECT COALESCE(MAX(load_start), '1900-01-01'::timestamptz)
            FROM meta.load_log
            WHERE status='success' 
                 AND load_start>=current_date-7
                AND table_name='{table_name}'
        """)
        return cur.fetchone()[0]

def update_load_log(conn_target, table_name, load_start, rows_loaded, status):
    with conn_target.cursor() as cur:
        cur.execute("""
            INSERT INTO meta.load_log (table_name, load_start, load_end, rows_loaded, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (table_name, load_start, datetime.now(), rows_loaded, status))
        conn_target.commit()

# Расчет для stg таблицы

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
                # 1. Очищаем таблицу (удаляем все старые данные)
                cur_tgt.execute("TRUNCATE TABLE stg.expense;")
                
                # 2. Вставляем новые данные (пакетно)
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
        
def get_city_id(category: str):
    """
    Определяет city_id по категории:
    - Если в category найдено слово 'Москва', 'мск', 'московская' → возвращает 56
    - Если найдено 'Санкт-Петербург', 'спб', 'питер', 'ленинград' → возвращает 324
    Иначе → None
    """
    if category is None:
        return None
    cat_lower = category.lower()
    
    # Москва → 56
    if any(word in cat_lower for word in ['москва', 'мск', 'московская']):
        return 56
    
    # Санкт-Петербург → 324
    if any(word in cat_lower for word in ['санкт-петербург', 'спб', 'питер', 'ленинград']):
        return 324
    
    return None

# Расчет для dwh таблицы
def load_fact_cashflow(**context):
    """
    Читает все записи из stg.expense, дополняет их city_id, date_id, hash_key
    и выполняет UPSERT в dwh.fact_cashflow по первичному ключу hash_key.
    """
    target_hook = PostgresHook(postgres_conn_id='DE2026_conn')
    conn_target = target_hook.get_conn()
    
    try:
        # 1. Получаем все строки из staging
        with conn_target.cursor() as cur:
            cur.execute("""
                SELECT id, datetime, amount, category
                FROM stg.expense
            """)
            rows = cur.fetchall()
        
        if not rows:
            logging.info("No data in stg.expense, skipping fact load")
            return
        
        # 2. Для каждой строки вычисляем дополнительные поля
        fact_rows = []
        for (cashflow_id, dt, amount, category) in rows:
            # operation_id фиксирован
            operation_id = 2
            # city_id по категории
            city_id = get_city_id(category)
            # date_id из datetime (формат YYYYMMDD)
            date_id = int(dt.strftime('%Y%m%d')) if dt else None
            # deal_id пока не заполняем
            deal_id = None
            # hash_key: md5 от комбинации уникальных полей
            unique_str = f"{cashflow_id}_{operation_id}_{date_id}_{city_id}"
            hash_key = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
            
            fact_rows.append((hash_key, cashflow_id, operation_id, city_id, deal_id, date_id, amount))
        
        # 3. Пакетная вставка с игнорированием конфликтов (MERGE через DO NOTHING)
        with conn_target.cursor() as cur:
            from psycopg2.extras import execute_values
            
            execute_values(cur, """
                INSERT INTO dwh.fact_cashflow 
                    (hash_key, cashflow_id, operation_id, city_id, deal_id, date_id, amount)
                VALUES %s
                ON CONFLICT (hash_key) DO NOTHING
            """, fact_rows)
            conn_target.commit()
            
            inserted_count = cur.rowcount  # количество реально вставленных строк
            logging.info(f"Inserted {inserted_count} new rows into dwh.fact_cashflow (skipped duplicates)")
        
        # 4. Логируем загрузку витрины
        load_start = datetime.now()
        update_load_log(conn_target, 'dwh.fact_cashflow', load_start, len(fact_rows), 'success')
        
    except Exception as e:
        conn_target.rollback()
        update_load_log(conn_target, 'dwh.fact_cashflow', datetime.now(), 0, 'failed')
        logging.error(f"Error in load_fact_cashflow: {e}")
        raise
    finally:
        conn_target.close()

with DAG(
    dag_id='dag_expense_to_cashflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['stg', 'de2026', 'expense']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    load_staging_task = PythonOperator(
        task_id='full_refresh_expense_stg',
        python_callable=load_expense,
    )
    
    # Новая задача – расчёт витрины fact_cashflow
    load_fact_task = PythonOperator(
        task_id='load_fact_cashflow_incremental',
        python_callable=load_fact_cashflow,
    )
    start >> load_staging_task >> load_fact_task >> end
