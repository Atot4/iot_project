# app_core/db_manager.py
print("EXECUTING: app_core/db_manager.py")

import psycopg2
from psycopg2 import sql 
import threading
import logging
import datetime
import json
import os
import sys
from datetime import timezone
import time
from psycopg2.pool import ThreadedConnectionPool
import collections 
from collections import defaultdict 
import pandas as pd
from dateutil.relativedelta import relativedelta

try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
    from app_core.config import DB_CONFIG, SHIFTS, RUNNING_STATUSES, IDLE_STATUSES, OTHER_STATUSES 
except ImportError:
    logging.error("Could not import config.py. Please ensure it's in app_core directory or path is set.")
    DB_CONFIG = {} 
    SHIFTS = {}
    RUNNING_STATUSES = []
    IDLE_STATUSES = []
    OTHER_STATUSES = [] 

logger = logging.getLogger(__name__)

_verified_program_report_tables_in_session = set()  # Set untuk melacak tabel laporan program yang sudah diverifikasi
_verified_table_lock = threading.Lock()  # Lock untuk melindungi set ini

db_write_lock = threading.RLock()

def format_seconds_to_hhmm(seconds):
    if seconds is None:
        return "00:00"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours:02d}:{minutes:02d}"

def format_seconds_to_hhmmss(seconds):
    if seconds is None:
        return "00:00:00"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds_remainder = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds_remainder:02d}"

def parse_hhmm_to_seconds(hhmm_str):
    if not isinstance(hhmm_str, str) or ":" in hhmm_str:
        return 0.0
    try:
        parts = hhmm_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        return float(hours * 3600 + minutes * 60)
    except (ValueError, IndexError):
        logger.warning(f"Could not parse HH:MM string '{hhmm_str}' to seconds. Returning 0.0.")
        return 0.0

# Global connection pool     
db_pool = None

def init_db_pool():
    global db_pool
    if db_pool is None:
        try:
            db_pool = ThreadedConnectionPool(1, 300, **DB_CONFIG) 
            logger.info("Database connection pool initialized.")
        except psycopg2.Error as e:
            logger.critical(f"Error initializing database connection pool: {e}", exc_info=True)
            raise ConnectionError("Failed to initialize database connection pool.") from e
    return db_pool

def connect_db():
    global db_pool
    if db_pool is None:
        logger.error("DB pool is None. The application tried to get a connection before the pool was initialized.")
        return None
    try:
        conn = db_pool.getconn()
        return conn
    except psycopg2.Error as e:
        logger.critical(f"Error getting connection from pool: {e}", exc_info=True)
        return None
        
def close_db_connection(conn):
    global db_pool
    if conn and db_pool:
        db_pool.putconn(conn)
        
def init_db():
    logger.info("Initializing database tables...")
    
    if db_pool is None:
        logger.critical("Database connection pool is not initialized. Exiting.")
        sys.exit(1)

    current_dt_object = datetime.datetime.now(timezone.utc)

    if not create_status_log_table(get_status_log_table_name(current_dt_object)):
        logger.error("Failed to initialize status log table.")
        sys.exit(1)

    if not create_shift_metrics_table(get_shift_metrics_table_name(current_dt_object)):
        logger.error("Failed to initialize shift metrics table.")
        sys.exit(1)

    if not create_final_shift_metrics_table_if_not_exists(get_final_shift_metrics_table_name(current_dt_object)):
        logger.error("Failed to initialize final shift metrics table.")
        sys.exit(1)

    if not create_program_report_table_monthly(get_program_report_table_name(current_dt_object)):
        logger.error("Failed to initialize program report table.")
        sys.exit(1)

    if not create_sub_program_analysis_table_monthly(get_sub_program_analysis_table_name(current_dt_object)):
        logger.error("Failed to initialize program efficiency archive table.")
        sys.exit(1)

    if not create_program_loss_breakdown_reports_table(get_program_loss_breakdown_reports_table_name(current_dt_object)):
        logger.error("Failed to initialize program loss breakdown reports table.")
        sys.exit(1)
    
    if not create_program_loss_breakdown_per_piece_reports_table(get_program_loss_breakdown_per_piece_reports_table_name(current_dt_object)):
        logger.error("Failed to initialize program loss breakdown reports table.")
        sys.exit(1)
        
    if not create_main_program_analysis_table_monthly(get_main_program_analysis_table_name(current_dt_object)):
        logger.error("Failed to initialize main program report archive table.")
        sys.exit(1)
    
    logger.info("Database initialization complete.")

def get_status_log_table_name(dt_obj: datetime.datetime) -> str:
    return f"machine_status_log_{dt_obj.strftime('%Y_%m')}"

def get_shift_metrics_table_name(dt_obj: datetime.datetime) -> str:
    return f"shift_metrics_{dt_obj.strftime('%Y_%m')}"

def get_final_shift_metrics_table_name(dt_obj: datetime.datetime) -> str:
    return f"final_shift_metrics_{dt_obj.strftime('%Y_%m')}"

def get_program_report_table_name(dt_obj: datetime.datetime) -> str:
    return dt_obj.strftime("program_report_%Y_%m").lower()

def get_sub_program_analysis_table_name(dt_obj: datetime.datetime) -> str:
    return dt_obj.strftime("sub-program_analysis_%Y_%m").lower()

def get_program_loss_breakdown_reports_table_name(dt_obj: datetime.datetime) -> str:
    return dt_obj.strftime("loss_breakdown_%Y_%m").lower()

def get_program_loss_breakdown_per_piece_reports_table_name(dt_obj: datetime.datetime) -> str:
    return dt_obj.strftime("loss_breakdown_per_piece_%Y_%m").lower()
    
# --- BARU: Fungsi untuk mendapatkan nama tabel arsip program induk ---
def get_main_program_analysis_table_name(dt_obj: datetime.datetime) -> str:
    return dt_obj.strftime("main_program_analysis_%Y_%m").lower()
# --- AKHIR BARU ---

# --- BARU: Fungsi untuk membuat tabel arsip program induk ---
def create_main_program_analysis_table_monthly(table_name: str) -> bool:
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create main program report archive table '{table_name}'.")
                return False
            cur = conn.cursor()
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    machine_name VARCHAR(255) NOT NULL,
                    report_date DATE NOT NULL,
                    program_main_name VARCHAR(255) NOT NULL,
                    session_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    session_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    total_process_time_seconds REAL,
                    total_loss_time_seconds REAL,
                    cycle_time_seconds REAL,
                    quantity INTEGER,
                    notes TEXT,
                    notes_qty TEXT,
                    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (machine_name, program_main_name, session_start_time)
                );
                CREATE INDEX IF NOT EXISTS {idx_machine_date} ON {table_name_for_index_1} (machine_name, report_date);
                CREATE INDEX IF NOT EXISTS {idx_program_name} ON {table_name_for_index_2} (program_main_name);
            """).format(
                sql.Identifier(table_name),
                idx_machine_date=sql.Identifier(f"idx_{table_name}_machine_date"),
                table_name_for_index_1=sql.Identifier(table_name),
                idx_program_name=sql.Identifier(f"idx_{table_name}_program_name"),
                table_name_for_index_2=sql.Identifier(table_name)
            ))
            conn.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error creating main program report archive table '{table_name}': {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        except Exception as e:
            logger.critical(f"CRITICAL Error creating main program report archive table '{table_name}': {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        finally:
            if cur: cur.close()
            if conn: close_db_connection(conn)
# --- AKHIR BARU ---

def save_main_program_analysis(machine_name: str, report_date: datetime.date, df_main_program_report: pd.DataFrame) -> bool:
    if df_main_program_report.empty:
        logger.info(f"No main program Analysis report data for {machine_name} on {report_date} to archive.")
        return True

    table_name = get_main_program_analysis_table_name(datetime.datetime.now(timezone.utc))
    
    if not create_main_program_analysis_table_monthly(table_name):
        logger.error(f"Failed to ensure main program archive table '{table_name}' exists before saving.")
        return False

    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database for saving main program report for {machine_name} on {report_date}.")
                return False
            cur = conn.cursor()

            insert_query = sql.SQL("""
                INSERT INTO {} (
                    machine_name, report_date, program_main_name, session_start_time, 
                    session_end_time, total_process_time_seconds, total_loss_time_seconds, 
                    cycle_time_seconds, quantity, notes, notes_qty
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (machine_name, program_main_name, session_start_time) DO UPDATE SET
                    session_end_time = EXCLUDED.session_end_time,
                    total_process_time_seconds = EXCLUDED.total_process_time_seconds,
                    total_loss_time_seconds = EXCLUDED.total_loss_time_seconds,
                    cycle_time_seconds = EXCLUDED.cycle_time_seconds,
                    quantity = EXCLUDED.quantity,
                    notes = EXCLUDED.notes,
                    notes_qty = EXCLUDED.notes_qty,               
                    archived_at = CURRENT_TIMESTAMP;
            """).format(sql.Identifier(table_name))

            data_to_insert = []
            for _, row in df_main_program_report.iterrows():
                # Pastikan semua nilai memiliki tipe data yang benar dan tidak None
                data_to_insert.append((
                    machine_name,
                    report_date,
                    row['program_main_name'],
                    row['session_start_time'].astimezone(timezone.utc),
                    row['session_end_time'].astimezone(timezone.utc),
                    float(row.get('total_process_time_seconds', 0.0)),
                    float(row.get('total_loss_time_seconds', 0.0)),
                    float(row.get('cycle_time_seconds', 0.0)),
                    int(row.get('Quantity', 0)),
                    str(row.get('notes_induk', '')),
                    str(row.get('Catatan', ''))
                ))
            
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            logger.info(f"Successfully archived main program report for {machine_name} on {report_date} ({len(df_main_program_report)} sessions) to table '{table_name}'.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error saving main program report for {machine_name} on {report_date}: {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        except Exception as e:
            logger.critical(f"CRITICAL: An unexpected error occurred while saving main program report for {machine_name} on {report_date}: {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        finally:
            if cur: cur.close()
            if conn: close_db_connection(conn)
# --- AKHIR BARU ---

# --- BARU: Fungsi untuk mengambil laporan program induk dari arsip ---
def get_main_program_report(machine_name: str = None, start_date: datetime.date = None, end_date: datetime.date = None, program_name_filter: str = None) -> list:
    results = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error("Failed to connect to database to fetch main program report archive.")
            return results

        cur = conn.cursor()

        table_names_to_query = set()
        start_dt_for_table_iter = datetime.datetime.combine(start_date, datetime.time.min, tzinfo=timezone.utc) if start_date else datetime.datetime.now(timezone.utc).replace(day=1)
        end_dt_for_table_iter = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=timezone.utc) if end_date else datetime.datetime.now(timezone.utc)

        temp_date = start_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while temp_date <= end_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            table_names_to_query.add(get_main_program_analysis_table_name(temp_date))
            if temp_date.month == 12:
                temp_date = temp_date.replace(year=temp_date.year + 1, month=1)
            else:
                temp_date = temp_date.replace(month=temp_date.month + 1)
            if (temp_date.year - start_dt_for_table_iter.year) * 12 + (temp_date.month - start_dt_for_table_iter.month) > 24:
                logger.warning("Loop for archive table names exceeded 24 months, breaking to prevent infinite loop.")
                break

        query_columns = """
            id, machine_name, report_date, program_main_name, session_start_time, 
            session_end_time, total_process_time_seconds, total_loss_time_seconds, 
            cycle_time_seconds, quantity, notes, notes_qty, archived_at
        """
        base_query_template = sql.SQL(f"SELECT {query_columns} FROM {{}}")

        query_parts_conditions = []
        params = []

        if machine_name:
            query_parts_conditions.append(sql.SQL("machine_name = %s"))
            params.append(machine_name)
        if start_date:
            query_parts_conditions.append(sql.SQL("report_date >= %s"))
            params.append(start_date)
        if end_date:
            query_parts_conditions.append(sql.SQL("report_date <= %s"))
            params.append(end_date)
        if program_name_filter:
            query_parts_conditions.append(sql.SQL("program_main_name ILIKE %s"))
            params.append(f"%{program_name_filter}%")

        order_by_clause = sql.SQL(" ORDER BY session_start_time DESC, machine_name ASC, program_main_name ASC;")

        for table_name in sorted(list(table_names_to_query)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Main program archive table '{table_name}' does not exist. Skipping.")
                    continue
                
                current_table_query = base_query_template.format(sql.Identifier(table_name))
                
                if query_parts_conditions:
                    current_table_query = sql.SQL(" ").join([current_table_query, sql.SQL(" WHERE ") + sql.SQL(" AND ").join(query_parts_conditions)])
                
                current_table_query = sql.SQL(" ").join([current_table_query, order_by_clause])

                cur.execute(current_table_query, params)
                results.extend(cur.fetchall())

            except psycopg2.Error as e:
                logger.warning(f"Error fetching from main program archive table '{table_name}': {e}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching from main program archive table '{table_name}': {e}", exc_info=True)
                continue
        
        column_names_list = [col.strip() for col in query_columns.split(',') if col.strip()]
        list_of_dicts = []
        for row_tuple in results:
            if len(column_names_list) == len(row_tuple):
                row_dict = dict(zip(column_names_list, row_tuple))
                list_of_dicts.append(row_dict)
            else:
                logger.error(f"Mismatched column count in fetched row. Expected {len(column_names_list)}, got {len(row_tuple)} for row: {row_tuple}")
            
        logger.info(f"Fetched {len(list_of_dicts)} archived main program report entries for filters.")
        return list_of_dicts

    except Exception as e:
        logger.critical(f"CRITICAL Error fetching main program archive report from DB: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)
# --- AKHIR BARU ---

def create_sub_program_analysis_table_monthly(table_name: str) -> bool:
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create program efficiency archive table '{table_name}'.")
                return False
            cur = conn.cursor()
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    machine_name VARCHAR(255) NOT NULL,
                    report_date DATE NOT NULL,
                    program_name VARCHAR(255) NOT NULL,
                    actual_avg_duration_seconds REAL,
                    target_duration_seconds REAL,
                    efficiency_percent REAL,
                    efficiency_status VARCHAR(50),
                    actual_spindle_speed_mode INTEGER,
                    actual_feed_rate_mode INTEGER,
                    target_spindle_speed INTEGER,
                    target_feed_rate INTEGER,
                    notes TEXT,
                    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (machine_name, report_date, program_name)
                );
                CREATE INDEX IF NOT EXISTS {idx_machine_date} ON {table_name_for_index_1} (machine_name, report_date);
                CREATE INDEX IF NOT EXISTS {idx_program_name} ON {table_name_for_index_2} (program_name);
            """).format(
                sql.Identifier(table_name),
                idx_machine_date=sql.Identifier(f"idx_{table_name}_machine_date"),
                table_name_for_index_1=sql.Identifier(table_name),
                idx_program_name=sql.Identifier(f"idx_{table_name}_program_name"),
                table_name_for_index_2=sql.Identifier(table_name)
            ))
            conn.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error creating program efficiency archive table '{table_name}': {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        except Exception as e:
            logger.critical(f"CRITICAL Error creating program efficiency archive table '{table_name}': {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        finally:
            if cur: cur.close()
            if conn: close_db_connection(conn)

def save_sub_program_analysis_report(machine_name: str, report_date: datetime.date, df_efficiency: pd.DataFrame) -> bool:
    if df_efficiency.empty:
        logger.info(f"No efficiency data for {machine_name} on {report_date} to archive.")
        return True

    table_name = get_sub_program_analysis_table_name(datetime.datetime.now(timezone.utc))
    
    if not create_sub_program_analysis_table_monthly(table_name):
        logger.error(f"Failed to ensure archive table '{table_name}' exists before saving efficiency report.")
        return False

    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database for saving program efficiency report for {machine_name} on {report_date}.")
                return False
            cur = conn.cursor()

            insert_query = sql.SQL("""
                INSERT INTO {} (
                    machine_name, report_date, program_name, actual_avg_duration_seconds, 
                    target_duration_seconds, efficiency_percent, efficiency_status, 
                    actual_spindle_speed_mode, actual_feed_rate_mode, 
                    target_spindle_speed, target_feed_rate, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (machine_name, report_date, program_name) DO UPDATE SET
                    actual_avg_duration_seconds = EXCLUDED.actual_avg_duration_seconds,
                    target_duration_seconds = EXCLUDED.target_duration_seconds,
                    efficiency_percent = EXCLUDED.efficiency_percent,
                    efficiency_status = EXCLUDED.efficiency_status,
                    actual_spindle_speed_mode = EXCLUDED.actual_spindle_speed_mode, 
                    actual_feed_rate_mode = EXCLUDED.actual_feed_rate_mode,     
                    target_spindle_speed = EXCLUDED.target_spindle_speed,      
                    target_feed_rate = EXCLUDED.target_feed_rate,              
                    notes = EXCLUDED.notes,                                    
                    archived_at = CURRENT_TIMESTAMP;
            """).format(sql.Identifier(table_name))

            data_to_insert = []
            for _, row in df_efficiency.iterrows():
                data_to_insert.append((
                    machine_name,
                    report_date, 
                    row['program_name'],
                    float(row.get('actual_avg_duration_per_piece_seconds', 0.0)),
                    float(row.get('target_duration_seconds', 0.0)),
                    float(row.get('efficiency_percent', 0.0)),
                    str(row.get('efficiency_status', '')),
                    int(row.get('most_common_spindle_speed', 0)), 
                    int(row.get('most_common_feed_rate', 0)),     
                    int(row.get('target_spindle_speed', 0)),     
                    int(row.get('target_feed_rate', 0)),         
                    str(row.get('notes', ''))
                ))
            
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            logger.info(f"Successfully archived efficiency report for {machine_name} on {report_date} ({len(df_efficiency)} programs) to table '{table_name}'.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error saving program efficiency report for {machine_name} on {report_date}: {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        except Exception as e:
            logger.critical(f"CRITICAL: An unexpected error occurred while saving program efficiency report for {machine_name} on {report_date}: {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        finally:
            if cur: cur.close()
            if conn: close_db_connection(conn)

def get_sub_program_analysis_report(
    machine_name: str = None, 
    start_date: datetime.date = None, 
    end_date: datetime.date = None,
    program_name_filter: str = None
) -> list:
    results = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error("Failed to connect to database to fetch program efficiency archive report.")
            return results

        cur = conn.cursor()

        table_names_to_query = set()
        start_dt_for_table_iter = datetime.datetime.combine(start_date, datetime.time.min, tzinfo=timezone.utc) if start_date else datetime.datetime.now(timezone.utc).replace(day=1)
        end_dt_for_table_iter = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=timezone.utc) if end_date else datetime.datetime.now(timezone.utc)

        temp_date = start_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while temp_date <= end_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            table_names_to_query.add(get_sub_program_analysis_table_name(temp_date))
            if temp_date.month == 12:
                temp_date = temp_date.replace(year=temp_date.year + 1, month=1)
            else:
                temp_date = temp_date.replace(month=temp_date.month + 1)
            if (temp_date.year - start_dt_for_table_iter.year) * 12 + (temp_date.month - start_dt_for_table_iter.month) > 24:
                logger.warning("Loop for archive table names exceeded 24 months, breaking to prevent infinite loop.")
                break


        query_columns = """
            id, machine_name, report_date, program_name, actual_avg_duration_seconds, 
            target_duration_seconds, efficiency_percent, efficiency_status, 
            actual_spindle_speed_mode, actual_feed_rate_mode, 
            target_spindle_speed, target_feed_rate, notes, archived_at
        """
        base_query_template = sql.SQL(f"SELECT {query_columns} FROM {{}}")

        query_parts_conditions = []
        params = []

        if machine_name:
            query_parts_conditions.append(sql.SQL("machine_name = %s"))
            params.append(machine_name)
        if start_date:
            query_parts_conditions.append(sql.SQL("report_date >= %s"))
            params.append(start_date)
        if end_date:
            query_parts_conditions.append(sql.SQL("report_date <= %s"))
            params.append(end_date)
        if program_name_filter:
            query_parts_conditions.append(sql.SQL("program_name ILIKE %s"))
            params.append(f"%{program_name_filter}%")

        order_by_clause = sql.SQL(" ORDER BY report_date DESC, machine_name ASC, program_name ASC;")

        for table_name in sorted(list(table_names_to_query)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Archive table '{table_name}' does not exist. Skipping.")
                    continue
                
                current_table_query = base_query_template.format(sql.Identifier(table_name))
                
                if query_parts_conditions:
                    current_table_query = sql.SQL(" ").join([current_table_query, sql.SQL(" WHERE ") + sql.SQL(" AND ").join(query_parts_conditions)])
                
                current_table_query = sql.SQL(" ").join([current_table_query, order_by_clause])

                cur.execute(current_table_query, params)
                results.extend(cur.fetchall())

            except psycopg2.Error as e:
                logger.warning(f"Error fetching from archive table '{table_name}': {e}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching from archive table '{table_name}': {e}", exc_info=True)
                continue
        
        column_names_list = [col.strip() for col in query_columns.split(',') if col.strip()]
        list_of_dicts = []
        for row_tuple in results:
            if len(column_names_list) == len(row_tuple):
                row_dict = dict(zip(column_names_list, row_tuple))
                list_of_dicts.append(row_dict)
            else:
                logger.error(f"Mismatched column count in fetched row. Expected {len(column_names_list)}, got {len(row_tuple)} for row: {row_tuple}")
            
        logger.info(f"Fetched {len(list_of_dicts)} archived efficiency report entries for filters.")
        return list_of_dicts

    except Exception as e:
        logger.critical(f"CRITICAL Error fetching program efficiency archive report from DB: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

def create_status_log_table(table_name: str): 
    with db_write_lock: 
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create status log table '{table_name}'.")
                return False
            if conn:
                cur = conn.cursor()
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        id SERIAL PRIMARY KEY,
                        machine_name VARCHAR(255) NOT NULL,
                        timestamp_log TIMESTAMP WITH TIME ZONE NOT NULL,
                        status_text VARCHAR(255),
                        spindle_speed INTEGER,
                        feed_rate INTEGER,
                        current_program VARCHAR(255), 
                        raw_log_data JSONB, 
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (machine_name, timestamp_log)
                    );
                """).format(sql.Identifier(table_name)))
                conn.commit()
                logger.debug(f"Table '{table_name}' checked/created successfully.")
                return True
        except psycopg2.Error as e:
            logger.error(f"Error creating status log table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)

def create_shift_metrics_table(table_name: str) -> bool:
    with db_write_lock: 
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create shift metrics table '{table_name}'.")
                return False
            if conn:  
                cur = conn.cursor()
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        machine_name VARCHAR(255) NOT NULL,
                        shift_name VARCHAR(50) NOT NULL,
                        runtime_seconds REAL NOT NULL,
                        idletime_seconds REAL NOT NULL,
                        other_time_seconds REAL NOT NULL,
                        shift_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        shift_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (machine_name, shift_name, shift_start_time)
                    );
                """).format(sql.Identifier(table_name)))
                conn.commit()
                logger.debug(f"Table '{table_name}' checked/created successfully.")
                return True
        except psycopg2.Error as e:
            logger.error(f"Error creating real-time shift metrics table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)
def create_final_shift_metrics_table_if_not_exists(table_name: str):
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create final shift metrics table '{table_name}'.")
                return False
            if conn:
                cur = conn.cursor()
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        id SERIAL PRIMARY KEY,
                        machine_name VARCHAR(255) NOT NULL,
                        shift_name VARCHAR(50) NOT NULL,
                        runtime_seconds REAL NOT NULL,
                        idletime_seconds REAL NOT NULL,
                        other_time_seconds REAL NOT NULL,
                        shift_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        shift_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        date_saved TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (machine_name, shift_start_time)
                    );
                """).format(sql.Identifier(table_name)))
                conn.commit()
                logger.info(f"Table '{table_name}' checked/created successfully.")
                return True
        except psycopg2.Error as e:
            logger.critical(f"Error creating final shift metrics table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)

def create_program_report_table_monthly(table_name: str) -> bool:
    with _verified_table_lock:
        if table_name in _verified_program_report_tables_in_session:
            logger.debug(f"Table '{table_name}' already verified in this session. Skipping creation.")
            return True
        
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create program report table '{table_name}'.")
                return False
            cur = conn.cursor()

            unique_constraint_name = f"unique_program_cycle_{table_name}"
            
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    machine_name VARCHAR(255) NOT NULL,
                    program_name VARCHAR(255) NOT NULL,
                    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    duration_seconds INTEGER NOT NULL,
                    report_date DATE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT {} UNIQUE (machine_name, program_name, start_time)
                );
                CREATE INDEX IF NOT EXISTS {idx_machine} ON {table_name_for_index_1} (machine_name);
                CREATE INDEX IF NOT EXISTS {idx_report_date} ON {table_name_for_index_2} (report_date);
                CREATE INDEX IF NOT EXISTS {idx_start_time} ON {table_name_for_index_3} (start_time);
            """).format(
                sql.Identifier(table_name),
                sql.Identifier(unique_constraint_name),
                idx_machine=sql.Identifier(f"idx_{table_name}_machine_name"), 
                table_name_for_index_1=sql.Identifier(table_name),
                idx_report_date=sql.Identifier(f"idx_{table_name}_report_date"), 
                table_name_for_index_2=sql.Identifier(table_name),
                idx_start_time=sql.Identifier(f"idx_{table_name}_start_time"), 
                table_name_for_index_3=sql.Identifier(table_name)
            ))
            
            conn.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
            with _verified_table_lock:
                _verified_program_report_tables_in_session.add(table_name)
            return True
        except psycopg2.Error as e:
            logger.error(f"Error creating/checking program_report table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        except Exception as e:
            logger.critical(f"CRITICAL Error creating/checking program_report table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur: 
                cur.close()
            if conn: 
                close_db_connection(conn)
                

def save_status_log(machine_name: str, timestamp: float, status_text: str, spindle_speed: int, feed_rate: int, current_program: str, table_name: str):
    with db_write_lock:
        start_lock_time = time.time()
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to save status log for {machine_name}.")
                return False
            if conn:
                cur = conn.cursor()
                dt_object = datetime.datetime.fromtimestamp(timestamp).astimezone(datetime.timezone.utc)
                    
                raw_log_data_dict = {
                    "timestamp": timestamp,
                    "status_text": status_text,
                    "spindle_speed": spindle_speed,
                    "feed_rate": feed_rate,
                    "current_program": current_program
                }
                raw_log_data_json = json.dumps(raw_log_data_dict, default=str)

                cur.execute(sql.SQL("""
                    INSERT INTO {} (machine_name, timestamp_log, status_text, spindle_speed, feed_rate, current_program, raw_log_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (machine_name, timestamp_log) DO NOTHING;
                """).format(sql.Identifier(table_name)),
                (machine_name, dt_object, status_text, spindle_speed, feed_rate, current_program, raw_log_data_json))
                conn.commit()
                if cur.rowcount > 0:
                    logger.debug(f"[{machine_name}] Successfully saved status log at {dt_object}.")
                    return True
                else:
                    logger.debug(f"[{machine_name}] Status log at {dt_object} already exists. Skipped.")
                    return True 
        except psycopg2.Error as e:
            logger.error(f"Error saving status log for {machine_name} to {table_name}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)
            end_lock_time = time.time()
            logger.debug(f"db_write_lock held for {end_lock_time - start_lock_time:.4f} seconds while saving status log for {machine_name}.")

def save_shift_metrics(machine_name: str, shift_name: str, runtime_sec: float, idletime_sec: float, other_time_sec: float, shift_start_time: datetime.datetime, shift_end_time: datetime.datetime, table_name: str):
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to save real-time shift metrics for {machine_name}.")
                return False
            if conn:
                cur = conn.cursor()
                shift_start_time_utc = shift_start_time.astimezone(datetime.timezone.utc)
                shift_end_time_utc = shift_end_time.astimezone(datetime.timezone.utc)

                upsert_query = sql.SQL("""
                    INSERT INTO {} (machine_name, shift_name, runtime_seconds, idletime_seconds, other_time_seconds, shift_start_time, shift_end_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (machine_name, shift_name, shift_start_time) DO UPDATE SET
                        runtime_seconds = EXCLUDED.runtime_seconds,
                        idletime_seconds = EXCLUDED.idletime_seconds,
                        other_time_seconds = EXCLUDED.other_time_seconds,
                        shift_end_time = EXCLUDED.shift_end_time,
                        last_updated = CURRENT_TIMESTAMP;
                """).format(sql.Identifier(table_name))

                cur.execute(upsert_query, (
                    machine_name,
                    shift_name,
                    round(runtime_sec, 2),
                    round(idletime_sec, 2),
                    round(other_time_sec, 2),
                    shift_start_time_utc,
                    shift_end_time_utc
                ))
                conn.commit()
                if cur.rowcount > 0:
                    logger.debug(f"Successfully saved/updated real-time shift metrics for {machine_name} - {shift_name} (Start: {shift_start_time.isoformat()}) to {table_name}.")
                    return True
                else:
                    logger.debug(f"Real-time shift metrics for {machine_name} - {shift_name} (Start: {shift_start_time.isoformat()}) unchanged. Skipped update.")
                    return True
        except psycopg2.Error as e:
            logger.error(f"Error saving/updating real-time shift metrics for {machine_name} - {shift_name} to {table_name}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)

def save_final_shift_metrics(machine_name: str, shift_name: str, runtime_sec: float, idletime_sec: float, other_time_sec: float, shift_start_time: datetime.datetime, shift_end_time: datetime.datetime):
    table_name = get_final_shift_metrics_table_name(shift_start_time)
    
    if not create_final_shift_metrics_table_if_not_exists(table_name):
        logger.error(f"Failed to ensure final shift metrics table '{table_name}' exists before saving.")
        return False
    
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to save final shift metrics for {machine_name}.")
                return False
            if conn:
                cur = conn.cursor()
                shift_start_time_utc = shift_start_time.astimezone(datetime.timezone.utc)
                shift_end_time_utc = shift_end_time.astimezone(datetime.timezone.utc)

                cur.execute(sql.SQL("""
                    INSERT INTO {} (machine_name, shift_name, runtime_seconds, idletime_seconds, other_time_seconds, shift_start_time, shift_end_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (machine_name, shift_start_time) DO NOTHING; 
                """).format(sql.Identifier(table_name)),
                (machine_name, shift_name, round(runtime_sec, 2), round(idletime_sec, 2), round(other_time_sec, 2), shift_start_time_utc, shift_end_time_utc))
                conn.commit()
                if cur.rowcount > 0:
                    logger.info(f"Final shift metrics for {machine_name} - {shift_name} (Start: {shift_start_time.isoformat()}) saved to {table_name}.")
                    return True
                else:
                    logger.debug(f"Final shift metrics for {machine_name} - {shift_name} (Start: {shift_start_time.isoformat()}) already exists. Skipped.")
                    return True
        except psycopg2.Error as e:
            logger.error(f"Error saving final shift metrics for {machine_name} - {shift_name} to {table_name}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            
            if conn:
                close_db_connection(conn)

def save_program_cycles_to_db(program_cycles_data: list) -> bool:
    if not program_cycles_data:
        logger.info("No program cycles data to save.")
        return True

    conn = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error("Failed to connect to database for saving program cycles.")
            return False
        cur = conn.cursor()

        grouped_by_table = defaultdict(list)
        for entry in program_cycles_data:
            table_name = get_program_report_table_name(entry['waktu_mulai'])
            grouped_by_table[table_name].append(entry)

        for table_name, entries_for_table in grouped_by_table.items():
            if not create_program_report_table_monthly(table_name):
                logger.warning(f"Skipping save for program cycles for table '{table_name}' as it could not be verified/created.")
                continue

            insert_query = sql.SQL("""
                INSERT INTO {} (machine_name, program_name, start_time, end_time, duration_seconds, report_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (machine_name, program_name, start_time) DO UPDATE SET
                    end_time = EXCLUDED.end_time,
                    duration_seconds = EXCLUDED.duration_seconds;
            """).format(sql.Identifier(table_name))

            data_to_insert = []
            for entry in entries_for_table:
                start_time_utc = entry['waktu_mulai'].astimezone(datetime.timezone.utc)
                end_time_utc = entry['waktu_selesai'].astimezone(datetime.timezone.utc)
                report_date_val = start_time_utc.date()
                data_to_insert.append((
                    entry['machine_name'],
                    entry['nama_program'],
                    start_time_utc,
                    end_time_utc,
                    int(entry['durasi_seconds']),
                    report_date_val
                ))

            cur.executemany(insert_query, data_to_insert)

        conn.commit()
        logger.info(f"Successfully saved {len(program_cycles_data)} program cycles to relevant monthly tables.")
        return True
    except psycopg2.Error as e:
        logger.error(f"Database error saving program cycles: {e}", exc_info=True)
        if conn: conn.rollback()
        return False
    except Exception as e:
        logger.critical(f"CRITICAL: An unexpected error occurred while saving program cycles: {e}", exc_info=True)
        if conn: conn.rollback()
        return False
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

def get_status_logs_for_machine(machine_name: str, start_time: datetime.datetime, end_time: datetime.datetime) -> list:
    logs = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error(f"Failed to connect to database to fetch status logs for {machine_name}.")
            return logs

        cur = conn.cursor()

        table_names_to_query = set()
        current_dt_iter = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt_for_iter = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=32)
        end_dt_for_iter = end_dt_for_iter.replace(day=1)
        
        
        while current_dt_iter <= end_dt_for_iter:
            table_names_to_query.add(get_status_log_table_name(current_dt_iter))
            if current_dt_iter.month == 12:
                current_dt_iter = current_dt_iter.replace(year=current_dt_iter.year + 1, month=1)
            else:
                current_dt_iter = current_dt_iter.replace(month=current_dt_iter.month + 1)

        start_time_utc = start_time.astimezone(datetime.timezone.utc)
        end_time_utc = end_time.astimezone(datetime.timezone.utc)

        for table_name in sorted(list(table_names_to_query)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name))) 
                if cur.fetchone()[0] is None:
                    logger.debug(f"Table '{table_name}' does not exist. Skipping.")
                    continue
                
                query = sql.SQL("""
                    SELECT timestamp_log, status_text, spindle_speed, feed_rate, current_program
                    FROM {}
                    WHERE machine_name = %s
                    AND timestamp_log >= %s AND timestamp_log < %s
                    ORDER BY timestamp_log ASC;
                """).format(sql.Identifier(table_name))

                cur.execute(query, (machine_name, start_time_utc, end_time_utc))
                records = cur.fetchall()

                for record in records:
                    timestamp_float = record[0].timestamp()
                    status_text = record[1]
                    spindle_speed = record[2]
                    feed_rate = record[3]
                    current_program = record[4]

                    log_entry = {
                        "timestamp": timestamp_float,
                        "status_text": status_text,
                        "spindle_speed": spindle_speed,
                        "feed_rate": feed_rate,
                        "current_program": current_program 
                    }
                    logs.append(log_entry)
            
            except psycopg2.ProgrammingError as e:
                if "column \"current_program\" does not exist" in str(e):
                    logger.warning(f"Table '{table_name}' is missing 'current_program' column. Please run ALTER TABLE or recreate table if data loss is acceptable. Attempting to fetch without it.")
                    try:
                        query_without_program = sql.SQL("""
                            SELECT timestamp_log, status_text, spindle_speed, feed_rate
                            FROM {}
                            WHERE machine_name = %s
                            AND timestamp_log >= %s AND timestamp_log < %s
                            ORDER BY timestamp_log ASC;
                        """).format(sql.Identifier(table_name))
                        cur.execute(query_without_program, (machine_name, start_time_utc, end_time_utc))
                        records_no_program = cur.fetchall()
                        for record in records_no_program:
                            timestamp_float = record[0].timestamp()
                            status_text = record[1]
                            spindle_speed = record[2]
                            feed_rate = record[3]
                            log_entry = {
                                "timestamp": timestamp_float,
                                "status_text": status_text,
                                "spindle_speed": spindle_speed,
                                "feed_rate": feed_rate,
                                "current_program": None
                            }
                            logs.append(log_entry)
                    except Exception as re_e:
                        logger.error(f"Error re-fetching status logs from table '{table_name}' without 'current_program': {re_e}", exc_info=True)
                else:
                    logger.error(f"Error fetching status logs from table '{table_name}' for {machine_name}: {e}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching status logs from table '{table_name}' for {machine_name}: {e}", exc_info=True)
                continue
        
        logger.debug(f"Fetched {len(logs)} status logs for {machine_name} from {start_time.isoformat()} to {end_time.isoformat()}.")
        return logs
    except Exception as e:
        logger.critical(f"CRITICAL Error fetching status logs from DB for {machine_name}: {e}", exc_info=True)
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            close_db_connection(conn)
def get_shift_metrics_from_db(machine_name: str = None, shift_name: str = None, start_date: datetime.date = None, end_date: datetime.date = None, is_final: bool = False) -> list:
    results = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            return results

        cur = conn.cursor()

        table_names_to_query = set()
        if start_date is None:
            start_date = datetime.date.today()
        if end_date is None:
            end_date = datetime.date.today()
        
        current_month_iter = datetime.datetime.combine(start_date.replace(day=1), datetime.time.min)
        effective_end_dt_for_iter = datetime.datetime.combine(end_date, datetime.time.max)
        while current_month_iter.date() <= effective_end_dt_for_iter.date().replace(day=1):
            if is_final:
                table_names_to_query.add(get_final_shift_metrics_table_name(current_month_iter))
            else:
                table_names_to_query.add(get_shift_metrics_table_name(current_month_iter))
            
            if current_month_iter.month == 12:
                current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
            else:
                current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)

        table_names_to_query = sorted(list(table_names_to_query))

        for table_name in table_names_to_query:
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Table '{table_name}' does not exist. Skipping.")
                    continue

                query_parts = [
                    sql.SQL("SELECT machine_name, shift_name, runtime_seconds, idletime_seconds, other_time_seconds, shift_start_time, shift_end_time FROM {}").format(sql.Identifier(table_name))
                ]
                conditions = []
                params = []
                
                query_start_dt_utc = datetime.datetime.combine(start_date, datetime.time.min).astimezone(datetime.timezone.utc)
                query_end_dt_utc = datetime.datetime.combine(end_date, datetime.time.max).astimezone(datetime.timezone.utc)

                if machine_name:
                    conditions.append(sql.SQL("machine_name = %s"))
                    params.append(machine_name)
                if shift_name:
                    conditions.append(sql.SQL("shift_name = %s"))
                    params.append(shift_name)
                conditions.append(sql.SQL("shift_start_time >= %s"))
                params.append(query_start_dt_utc)
                conditions.append(sql.SQL("shift_start_time <= %s"))
                params.append(query_end_dt_utc)

                if conditions:
                    query_parts.append(sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions))
                
                query_parts.append(sql.SQL(" ORDER BY shift_start_time DESC;"))
                
                final_query = sql.SQL("").join(query_parts)
                
                cur.execute(final_query, params)
                records = cur.fetchall()
                
                column_names = [desc[0] for desc in cur.description]
                
                for record in records:
                    row_dict = dict(zip(column_names, record))
                    row_dict['runtime_hhmm'] = format_seconds_to_hhmm(row_dict.get('runtime_seconds'))
                    row_dict['idletime_hhmm'] = format_seconds_to_hhmm(row_dict.get('idletime_seconds'))
                    results.append(row_dict)
            
            except psycopg2.Error as e:
                logger.warning(f"Error fetching from table '{table_name}': {e}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching from table '{table_name}': {e}", exc_info=True)
                continue
        
        logger.info(f"Fetched {len(results)} shift metrics (is_final={is_final}) from {len(table_names_to_query)} tables.")
        return results
    except Exception as e:
        logger.critical(f"CRITICAL Error fetching shift metrics from DB (is_final={is_final}): {e}", exc_info=True)
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            close_db_connection(conn)

def get_program_report_from_db2(machine_name: str, start_date: datetime.date, end_date: datetime.date, specific_program_filter: str):
    logs = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error(f"Failed to connect to database to fetch program run span for {machine_name}.")
            return logs

        cur = conn.cursor()

        table_names_to_query_status_log = set()
        current_dt_iter_span = datetime.datetime.combine(start_date.replace(day=1), datetime.time.min, tzinfo=timezone.utc)
        end_dt_for_iter_span = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=timezone.utc)
        
        while current_dt_iter_span <= end_dt_for_iter_span.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            table_names_to_query_status_log.add(get_status_log_table_name(current_dt_iter_span))
            if current_dt_iter_span.month == 12:
                current_dt_iter_span = current_dt_iter_span.replace(year=current_dt_iter_span.year + 1, month=1)
            else:
                current_dt_iter_span = current_dt_iter_span.replace(month=current_dt_iter_span.month + 1)
            if (current_dt_iter_span.year - start_date.year) * 12 + (current_dt_iter_span.month - start_date.month) > 24:
                logger.warning("Loop for status log table names exceeded 24 months, breaking to prevent infinite loop.")
                break

        min_timestamp_span = None
        max_timestamp_span = None

        for table_name_status_log in sorted(list(table_names_to_query_status_log)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name_status_log)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Status log table '{table_name_status_log}' does not exist. Skipping timestamp span search.")
                    continue

                min_max_ts_query = sql.SQL("""
                    SELECT MIN(timestamp_log), MAX(timestamp_log)
                    FROM {}
                    WHERE machine_name = %s
                    AND current_program ILIKE %s
                    AND status_text = 'Running'
                    AND timestamp_log >= %s AND timestamp_log <= %s;
                """).format(sql.Identifier(table_name_status_log))

                cur.execute(min_max_ts_query, (machine_name, f"{specific_program_filter}%", 
                                             datetime.datetime.combine(start_date, datetime.time.min).astimezone(timezone.utc), 
                                             datetime.datetime.combine(end_date, datetime.time.max).astimezone(timezone.utc)))
                
                min_ts, max_ts = cur.fetchone()

                if min_ts and max_ts:
                    if min_timestamp_span is None or min_ts < min_timestamp_span:
                        min_timestamp_span = min_ts
                    if max_timestamp_span is None or max_ts > max_timestamp_span:
                        max_timestamp_span = max_ts
            except psycopg2.ProgrammingError as e:
                if "column \"current_program\" does not exist" in str(e):
                    logger.warning(f"Table '{table_name_status_log}' missing 'current_program' column. Cannot search by program name.")
                else:
                    logger.warning(f"Error searching timestamp span in status log table '{table_name_status_log}': {e}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred during timestamp span search in status log table '{table_name_status_log}': {e}", exc_info=True)
                continue

        if min_timestamp_span is None or max_timestamp_span is None:
            logger.info(f"No running timestamp span found for program '{specific_program_filter}' on {machine_name} between {start_date} and {end_date}.")
            return [] 

        all_relevant_logs_in_span = []
        current_dt_iter_fetch = min_timestamp_span.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        while current_dt_iter_fetch <= max_timestamp_span.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            table_name_to_fetch = get_status_log_table_name(current_dt_iter_fetch)
            
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name_to_fetch)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Status log table '{table_name_to_fetch}' does not exist during span fetch. Skipping.")
                else: 
                    fetch_span_query = sql.SQL("""
                        SELECT timestamp_log, status_text, spindle_speed, feed_rate, current_program
                        FROM {}
                        WHERE machine_name = %s
                        AND timestamp_log >= %s AND timestamp_log <= %s
                        ORDER BY timestamp_log ASC;
                    """).format(sql.Identifier(table_name_to_fetch))

                    cur.execute(fetch_span_query, (machine_name, min_timestamp_span, max_timestamp_span))
                    all_relevant_logs_in_span.extend(cur.fetchall())
            except psycopg2.ProgrammingError as e:
                if "column \"current_program\" does not exist" in str(e):
                    logger.warning(f"Table '{table_name_to_fetch}' missing 'current_program' column. Fetching without it.")
                    fetch_span_query_no_program = sql.SQL("""
                        SELECT timestamp_log, status_text, spindle_speed, feed_rate
                        FROM {}
                        WHERE machine_name = %s
                        AND timestamp_log >= %s AND timestamp_log <= %s
                        ORDER BY timestamp_log ASC;
                    """).format(sql.Identifier(table_name_to_fetch))
                    cur.execute(fetch_span_query_no_program, (machine_name, min_timestamp_span, max_timestamp_span))
                    fetched_rows = cur.fetchall()
                    all_relevant_logs_in_span.extend([row + (None,) for row in fetched_rows])
                else:
                    logger.error(f"Error fetching span from status log table '{table_name_to_fetch}': {e}", exc_info=True)
            except Exception as e:
                logger.error(f"An unexpected error occurred during span fetch from status log table '{table_name_to_fetch}': {e}", exc_info=True)
                
            if current_dt_iter_fetch.month == 12:
                current_dt_iter_fetch = current_dt_iter_fetch.replace(year=current_dt_iter_fetch.year + 1, month=1)
            else:
                current_dt_iter_fetch = current_dt_iter_fetch.replace(month=current_dt_iter_fetch.month + 1)
            if (current_dt_iter_fetch.year - min_timestamp_span.year) * 12 + (current_dt_iter_fetch.month - min_timestamp_span.month) > 24: 
                logger.warning("Span fetch loop exceeded 24 months, breaking.")
                break

        columns = ['timestamp', 'status_text', 'spindle_speed', 'feed_rate', 'current_program']
        result_list = []
        for row_tuple in all_relevant_logs_in_span:
            if len(columns) == len(row_tuple):
                row_dict = dict(zip(columns, row_tuple))
                result_list.append(row_dict)
            else:
                logger.error(f"Mismatched column count in fetched span row. Expected {len(columns)}, got {len(row_tuple)} for row: {row_tuple}")

        logger.info(f"Fetched {len(result_list)} status logs within timestamp span for program '{specific_program_filter}' on {machine_name}.")
        return result_list

    except Exception as e:
        logger.critical(f"CRITICAL Error in get_program_report_from_db2 for {specific_program_filter}: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

def get_program_report_from_db(machine_name: str, start_date: datetime.date, end_date: datetime.date):
    conn = None
    cur = None
    all_records = []
    try:
        conn = connect_db()
        if conn is None:
            logger.error(f"Failed to connect to database to fetch program report for {machine_name}.")
            return all_records

        cur = conn.cursor()

        table_names_to_query = set()
        current_iter_date = datetime.datetime.combine(start_date.replace(day=1), datetime.time.min, tzinfo=datetime.timezone.utc)
        end_dt_for_iter = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=datetime.timezone.utc)
        end_dt_for_iter_month_start = end_dt_for_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=32)
        end_dt_for_iter_month_start = end_dt_for_iter_month_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        while current_iter_date <= end_dt_for_iter_month_start:
            table_names_to_query.add(get_program_report_table_name(current_iter_date))
            if current_iter_date.month == 12:
                current_iter_date = current_iter_date.replace(year=current_iter_date.year + 1, month=1)
            else:
                current_iter_date = current_iter_date.replace(month=current_iter_date.month + 1)      

        start_time_utc = datetime.datetime.combine(start_date, datetime.time.min).astimezone(datetime.timezone.utc)
        end_time_utc = datetime.datetime.combine(end_date, datetime.time.max).astimezone(datetime.timezone.utc)

        for table_name in sorted(list(table_names_to_query)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Table '{table_name}' does not exist. Skipping.")
                    continue

                select_query = sql.SQL("""
                    SELECT machine_name, program_name, start_time, end_time, duration_seconds
                    FROM {}
                    WHERE machine_name = %s
                    AND start_time >= %s AND start_time < %s
                    ORDER BY start_time;
                """).format(sql.Identifier(table_name))
                
                cur.execute(select_query, (machine_name, start_time_utc, end_time_utc))
                all_records.extend(cur.fetchall())

            except psycopg2.Error as e:
                logger.warning(f"Error fetching from program report table '{table_name}': {e}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching from program report table '{table_name}': {e}", exc_info=True)
                continue

        columns = ['machine_name', 'program_name', 'start_time', 'end_time', 'duration_seconds']
        result_list = []
        for row in all_records:
            row_dict = {}
            for i, col_name in enumerate(columns):
                row_dict[col_name] = row[i]
            result_list.append(row_dict)

        logger.info(f"Fetched {len(result_list)} program report entries for {machine_name} from {start_date} to {end_date}.")
        return result_list

    except Exception as e:
        logger.critical(f"CRITICAL Error fetching program report from DB: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn:
            close_db_connection(conn)

def check_and_save_completed_shifts(shift_metrics_data: dict, current_time: datetime.datetime, shifts_saved_state: dict, shift_metrics_lock: threading.Lock, shifts_saved_to_db_lock: threading.Lock):
    messages = []
    newly_saved_shifts = []

    for machine_name, shifts_info in shift_metrics_data.items():
        for shift_name, metrics in shifts_info.items():
            shift_start_iso = metrics.get("shift_start")
            shift_end_iso = metrics.get("shift_end")

            if not shift_start_iso or not shift_end_iso:
                logger.warning(f"Skipping shift {shift_name} for {machine_name} due to missing start/end time in metrics.")
                continue

            try:
                shift_start_dt = datetime.datetime.fromisoformat(shift_start_iso)
                shift_end_dt = datetime.datetime.fromisoformat(shift_end_iso)
            except ValueError:
                logger.error(f"Invalid ISO format for shift times for {machine_name} - {shift_name}. Skipping.")
                continue

            shift_unique_id = f"{machine_name}_{shift_name}_{shift_start_iso}"

            with shifts_saved_to_db_lock:
                already_saved = shifts_saved_state.get(shift_unique_id, False)

            if current_time >= shift_end_dt and not already_saved:
                logger.info(f"Shift {shift_name} for {machine_name} has completed. Saving final metrics...")
                
                runtime_sec = metrics.get("runtime_seconds", 0.0)
                idletime_sec = metrics.get("idletime_seconds", 0.0)

                total_shift_duration_seconds = (shift_end_dt - shift_start_dt).total_seconds()
                accounted_time_seconds = runtime_sec + idletime_sec
                other_time_sec = max(0.0, total_shift_duration_seconds - accounted_time_seconds)

                if save_final_shift_metrics(
                    machine_name,
                    shift_name,
                    runtime_sec,
                    idletime_sec,
                    other_time_sec,
                    shift_start_dt,
                    shift_end_dt
                ):
                    with shifts_saved_to_db_lock:
                        shifts_saved_state[shift_unique_id] = True
                    messages.append(f"Successfully saved final metrics for {machine_name} - {shift_name} ({shift_start_iso}).")
                    newly_saved_shifts.append(shift_unique_id)
                else:
                    messages.append(f"Failed to save final metrics for {machine_name} - {shift_name} ({shift_start_iso}).")
            elif current_time >= shift_end_dt and already_saved:
                logger.debug(f"Shift {shift_name} for {machine_name} (Start: {shift_start_iso}) is ended and already marked as saved.")
            else:
                logger.debug(f"Shift {shift_name} for {machine_name} is still ongoing or not yet completed. Current time: {current_time.isoformat()}, Shift end: {shift_end_dt.isoformat()}")

    return messages, newly_saved_shifts

def update_program_name_in_db(old_program_name, new_program_name, machine_name, start_date, end_date):
    if not db_pool:
        logging.error("Database connection pool not initialized.")
        return False
        
    conn = None
    all_updates_successful = True
    
    current_date = start_date
    table_names_to_update = set()
    while current_date <= end_date:
        table_name = f"program_report_{current_date.strftime('%Y_%m')}"
        table_names_to_update.add(table_name)
        current_date += relativedelta(months=1)
    
    try:
        conn = db_pool.getconn()
        
        for table_name in table_names_to_update:
            with conn.cursor() as cur:
                check_sql = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
                cur.execute(check_sql)
                table_exists = cur.fetchone()[0]

                if table_exists:
                    sql = f"""
                        UPDATE "{table_name}"
                        SET program_name = %s
                        WHERE machine_name = %s AND program_name = %s;
                    """
                    try:
                        cur.execute(sql, (new_program_name, machine_name, old_program_name))
                        logging.info(f"Updated program name from '{old_program_name}' to '{new_program_name}' on table '{table_name}'. Rows affected: {cur.rowcount}")
                    except Exception as update_e:
                        logging.error(f"Error updating table '{table_name}': {update_e}", exc_info=True)
                        all_updates_successful = False
                else:
                    logging.warning(f"Table '{table_name}' does not exist. Skipping update for this table.")
                    
        conn.commit()
        return all_updates_successful
        
    except Exception as e:
        logging.error(f"Critical error during program name update transaction: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            db_pool.putconn(conn)


def save_loss_breakdown_report(machine_name, report_date, df_loss_breakdown):
    """
    Menyimpan rincian waktu loss ke database.
    """
    if df_loss_breakdown.empty:
        logging.warning("Tidak ada data loss breakdown untuk disimpan.")
        return True 

    table_name = get_program_loss_breakdown_reports_table_name(datetime.datetime.now(timezone.utc))

    if not create_program_loss_breakdown_reports_table(table_name):
        logger.error(f"Failed to ensure loss breakdown archive table '{table_name}' exists before saving.")
        return False

    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = db_pool.getconn()
            if conn is None:
                logger.error("Failed to connect to database for saving loss breakdown report.")
                return False
            cur = conn.cursor()

            sql_query = sql.SQL("""
                INSERT INTO {} (
                    machine_name,
                    report_date,
                    loss_category,
                    duration_seconds
                ) VALUES (
                    %s, %s, %s, %s
                ) ON CONFLICT (machine_name, report_date, loss_category) DO UPDATE SET
                    duration_seconds = EXCLUDED.duration_seconds;
            """).format(sql.Identifier(table_name))

            data_to_insert = []
            for _, row in df_loss_breakdown.iterrows():
                data_to_insert.append((
                    machine_name,
                    report_date,
                    row['Category'],
                    float(row['Duration (seconds)'])
                ))

            cur.executemany(sql_query, data_to_insert)
            conn.commit()
            logger.info(f"Successfully saved loss breakdown report for {machine_name} to table '{table_name}'.")
            return True
        except Exception as e:
            logger.error(f"Error saving loss breakdown report: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                db_pool.putconn(conn)


def save_loss_breakdown_per_piece_report(machine_name, report_date, df_loss_breakdown_per_piece):
    """
    Menyimpan rincian waktu loss ke database.
    """
    if df_loss_breakdown_per_piece.empty:
        logging.warning("Tidak ada data loss breakdown untuk disimpan.")
        return True 

    table_name = get_program_loss_breakdown_per_piece_reports_table_name(datetime.datetime.now(timezone.utc))

    if not create_program_loss_breakdown_per_piece_reports_table(table_name):
        logger.error(f"Failed to ensure loss breakdown archive table '{table_name}' exists before saving.")
        return False

    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = db_pool.getconn()
            if conn is None:
                logger.error("Failed to connect to database for saving loss breakdown report.")
                return False
            cur = conn.cursor()

            sql_query = sql.SQL("""
                INSERT INTO {} (
                    machine_name,
                    report_date,
                    loss_category,
                    duration_seconds
                ) VALUES (
                    %s, %s, %s, %s
                ) ON CONFLICT (machine_name, report_date, loss_category) DO UPDATE SET
                    duration_seconds = EXCLUDED.duration_seconds;
            """).format(sql.Identifier(table_name))

            data_to_insert = []
            for _, row in df_loss_breakdown_per_piece.iterrows():
                data_to_insert.append((
                    machine_name,
                    report_date,
                    row['Category'],
                    float(row['Duration (seconds)'])
                ))

            cur.executemany(sql_query, data_to_insert)
            conn.commit()
            logger.info(f"Successfully saved loss breakdown report for {machine_name} to table '{table_name}'.")
            return True
        except Exception as e:
            logger.error(f"Error saving loss breakdown report: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                db_pool.putconn(conn)


def create_program_loss_breakdown_reports_table(table_name: str):
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create program loss breakdown reports table '{table_name}'.")
                return False
            cur = conn.cursor()
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    machine_name VARCHAR(255) NOT NULL,
                    report_date DATE NOT NULL,
                    loss_category VARCHAR(255) NOT NULL,
                    duration_seconds REAL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (machine_name, report_date, loss_category)
                );
            """).format(sql.Identifier(table_name)))
            conn.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error creating program loss breakdown reports table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)


def create_program_loss_breakdown_per_piece_reports_table(table_name: str):
    with db_write_lock:
        conn = None
        cur = None
        try:
            conn = connect_db()
            if conn is None:
                logger.error(f"Failed to connect to database to create program loss breakdown per piece reports table '{table_name}'.")
                return False
            cur = conn.cursor()
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    machine_name VARCHAR(255) NOT NULL,
                    report_date DATE NOT NULL,
                    loss_category VARCHAR(255) NOT NULL,
                    duration_seconds REAL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (machine_name, report_date, loss_category)
                );
            """).format(sql.Identifier(table_name)))
            conn.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error creating program loss breakdown per piece reports table '{table_name}': {e}", exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if cur:
                cur.close()
            if conn:
                close_db_connection(conn)


def get_loss_breakdown_report(machine_name: str = None, start_date: datetime.date = None, end_date: datetime.date = None) -> list:
    results = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error("Failed to connect to database to fetch loss breakdown archive.")
            return results

        cur = conn.cursor()

        table_names_to_query = set()
        start_dt_for_table_iter = datetime.datetime.combine(start_date, datetime.time.min, tzinfo=timezone.utc) if start_date else datetime.datetime.now(timezone.utc).replace(day=1)
        end_dt_for_table_iter = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=timezone.utc) if end_date else datetime.datetime.now(timezone.utc)

        temp_date = start_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while temp_date <= end_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            table_names_to_query.add(get_program_loss_breakdown_reports_table_name(temp_date))
            temp_date += relativedelta(months=1)
            
        query_columns = """
            id, machine_name, report_date, loss_category, duration_seconds
        """
        base_query_template = sql.SQL(f"SELECT {query_columns} FROM {{}}")
        query_parts_conditions = []
        params = []
        
        if machine_name:
            query_parts_conditions.append(sql.SQL("machine_name = %s"))
            params.append(machine_name)
        if start_date:
            query_parts_conditions.append(sql.SQL("report_date >= %s"))
            params.append(start_date)
        if end_date:
            query_parts_conditions.append(sql.SQL("report_date <= %s"))
            params.append(end_date)
            
        order_by_clause = sql.SQL(" ORDER BY report_date DESC, machine_name ASC;")
        
        for table_name in sorted(list(table_names_to_query)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Loss breakdown archive table '{table_name}' does not exist. Skipping.")
                    continue
                
                current_table_query = base_query_template.format(sql.Identifier(table_name))
                if query_parts_conditions:
                    current_table_query = sql.SQL(" ").join([current_table_query, sql.SQL(" WHERE ") + sql.SQL(" AND ").join(query_parts_conditions)])
                current_table_query = sql.SQL(" ").join([current_table_query, order_by_clause])

                cur.execute(current_table_query, params)
                results.extend(cur.fetchall())

            except psycopg2.Error as e:
                logger.warning(f"Error fetching from loss breakdown archive table '{table_name}': {e}", exc_info=True)
                continue
        
        column_names_list = [col.strip() for col in query_columns.split(',') if col.strip()]
        list_of_dicts = []
        for row_tuple in results:
            if len(column_names_list) == len(row_tuple):
                row_dict = dict(zip(column_names_list, row_tuple))
                list_of_dicts.append(row_dict)
            
        logger.info(f"Fetched {len(list_of_dicts)} archived loss breakdown entries for filters.")
        return list_of_dicts

    except Exception as e:
        logger.critical(f"CRITICAL Error fetching loss breakdown archive from DB: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)


def get_loss_breakdown_per_piece_report(machine_name: str = None, start_date: datetime.date = None, end_date: datetime.date = None) -> list:
    results = []
    conn = None
    cur = None
    try:
        conn = connect_db()
        if conn is None:
            logger.error("Failed to connect to database to fetch loss breakdown per piece archive.")
            return results

        cur = conn.cursor()

        table_names_to_query = set()
        start_dt_for_table_iter = datetime.datetime.combine(start_date, datetime.time.min, tzinfo=timezone.utc) if start_date else datetime.datetime.now(timezone.utc).replace(day=1)
        end_dt_for_table_iter = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=timezone.utc) if end_date else datetime.datetime.now(timezone.utc)

        temp_date = start_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while temp_date <= end_dt_for_table_iter.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            table_names_to_query.add(get_program_loss_breakdown_per_piece_reports_table_name(temp_date))
            temp_date += relativedelta(months=1)
            
        query_columns = """
            id, machine_name, report_date, loss_category, duration_seconds
        """
        base_query_template = sql.SQL(f"SELECT {query_columns} FROM {{}}")
        query_parts_conditions = []
        params = []
        
        if machine_name:
            query_parts_conditions.append(sql.SQL("machine_name = %s"))
            params.append(machine_name)
        if start_date:
            query_parts_conditions.append(sql.SQL("report_date >= %s"))
            params.append(start_date)
        if end_date:
            query_parts_conditions.append(sql.SQL("report_date <= %s"))
            params.append(end_date)
            
        order_by_clause = sql.SQL(" ORDER BY report_date DESC, machine_name ASC;")
        
        for table_name in sorted(list(table_names_to_query)):
            try:
                cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
                if cur.fetchone()[0] is None:
                    logger.debug(f"Loss breakdown per piece archive table '{table_name}' does not exist. Skipping.")
                    continue
                
                current_table_query = base_query_template.format(sql.Identifier(table_name))
                if query_parts_conditions:
                    current_table_query = sql.SQL(" ").join([current_table_query, sql.SQL(" WHERE ") + sql.SQL(" AND ").join(query_parts_conditions)])
                current_table_query = sql.SQL(" ").join([current_table_query, order_by_clause])

                cur.execute(current_table_query, params)
                results.extend(cur.fetchall())

            except psycopg2.Error as e:
                logger.warning(f"Error fetching from loss breakdown per piece archive table '{table_name}': {e}", exc_info=True)
                continue
        
        column_names_list = [col.strip() for col in query_columns.split(',') if col.strip()]
        list_of_dicts = []
        for row_tuple in results:
            if len(column_names_list) == len(row_tuple):
                row_dict = dict(zip(column_names_list, row_tuple))
                list_of_dicts.append(row_dict)
            
        logger.info(f"Fetched {len(list_of_dicts)} archived loss breakdown per piece entries for filters.")
        return list_of_dicts

    except Exception as e:
        logger.critical(f"CRITICAL Error fetching loss breakdown per piece archive from DB: {e}", exc_info=True)
        return []
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)