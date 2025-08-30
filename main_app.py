# main_app.py

from app_core.opc_client_module import OpcUaClient
import app_core.data_processor as data_processor
import app_core.shift_calculator as shift_calculator
import json
import time
import threading
import logging
import os
import datetime
from collections import defaultdict 
from datetime import timezone # Pastikan ini diimpor untuk datetime.timezone.utc
import app_core.program_processor as program_processor 

# Mengimpor konfigurasi dari app_core/config.py
from app_core.config import (
    SHIFTS,
    DATA_FILE,
    STATUS_LOG_RETENTION_HOURS,
    STATUS_LOG_DB_INTERVAL_SECONDS,
    DB_CONFIG,
)
# Mengimpor fungsi manajemen DB dari app_core/db_manager.py
from app_core.db_manager import (
    connect_db,
    create_status_log_table,
    save_status_log,
    get_status_logs_for_machine, 
    create_shift_metrics_table,
    save_shift_metrics,
    create_final_shift_metrics_table_if_not_exists,
    save_final_shift_metrics,
    check_and_save_completed_shifts,
    get_shift_metrics_table_name,
    get_status_log_table_name,
    get_program_report_table_name, 
    create_program_report_table_monthly,
    save_program_cycles_to_db, 
    init_db,
    init_db_pool,
    db_write_lock
)

# --- Konfigurasi Logging ---
# Logging setup
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(LOG_FORMAT)

# Root logger configuration
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Console handler for root logger
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(console_handler)

logging.getLogger('opcua').setLevel(logging.DEBUG)

# Specific loggers configuration
loggers_config = {
    '__main__': {'file': os.path.join(LOG_DIR, "main_app.log"), 'level': logging.WARNING},
    'app_core.db_manager': {'file': os.path.join(LOG_DIR, "db_manager.log"), 'level': logging.WARNING},
    'app_core.data_processor': {'file': os.path.join(LOG_DIR, "data_processor.log"), 'level': logging.DEBUG},
    'app_core.program_processor': {'file': os.path.join(LOG_DIR, "program_processor.log"), 'level': logging.WARNING},
    'app_core.shift_calculator': {'file': os.path.join(LOG_DIR, "shift_calculator.log"), 'level': logging.WARNING},
    'app_core.program_report_thread': {'file': os.path.join(LOG_DIR, "program_report_thread.log"), 'level': logging.WARNING},
    # 'app_core.opc_client_module': {'file': os.path.join(LOG_DIR, "opc_client_module.log"), 'level': logging.WARNING}, 
    'tzlocal': {'file': os.path.join(LOG_DIR, "tzlocal.log"), 'level': logging.WARNING},
}
for logger_name, config_setting in loggers_config.items():
    specific_logger = logging.getLogger(logger_name)
    specific_logger.setLevel(config_setting['level'])
    
    file_handler = logging.FileHandler(config_setting['file'])
    file_handler.setFormatter(formatter)
    specific_logger.addHandler(file_handler)
    specific_logger.propagate = False

# Get logger for main_app.py
logger = logging.getLogger(__name__)

latest_machine_data = {}
data_lock = threading.RLock()

machine_shift_metrics = {}
shift_metrics_lock = threading.RLock()

latest_status_for_db_write = {}
latest_status_for_db_write_lock = threading.Lock()

shifts_saved_to_db = {}
shifts_saved_to_db_lock = threading.RLock()

# --- Fungsi untuk Memuat Konfigurasi ---
def load_machine_configs(filepath):
    """
    Memuat konfigurasi mesin dari file JSON, termasuk konfigurasi global.
    """
    try:
        with open(filepath, "r") as f:
            full_config = json.load(f)

            url = full_config.get("url")
            user = os.getenv("OPC_UA_USER")
            password = os.getenv("OPC_UA_PASSWORD")

            machine_configs = full_config.get("machines", [])
            if not isinstance(machine_configs, list):
                raise ValueError(
                    "JSON 'machines' key must contain a list of machine configurations."
                )

            return machine_configs, url, user, password
    except FileNotFoundError:
        logger.error(f"Error: Configuration file '{filepath}' not found.")
        return [], None, None, None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from '{filepath}': {e}")
        return [], None, None, None
    except ValueError as e:
        logger.error(f"Configuration file format error: {e}")
        return [], None, None, None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading config: {e}")
        return [], None, None, None


# --- Fungsi untuk Polling Mesin (untuk Thread) ---
# def poll_machine_thread_target(client_instance, interval, stop_event):
#     """
#     Target fungsi untuk setiap thread. Mengelola koneksi dan polling untuk satu mesin.
#     """
#     max_retries = 3
#     retry_delay = 5

#     for attempt in range(max_retries):
#         if stop_event.is_set():
#             logger.info(
#                 f"[{client_instance.machine_name}] Stop event set, terminating before connection."
#             )
#             return

#         if client_instance.connect():
#             logger.info(
#                 f"[{client_instance.machine_name}] Starting polling and processing."
#             )
#             try:
#                 while not stop_event.is_set():
#                     raw_data = client_instance.read_all_variables()

#                     if raw_data is not None:
#                         logger.info(
#                             f"[{client_instance.machine_name}] Raw Data: {raw_data}"
#                         )

#                         processed_data = data_processor.process_opcua_data(
#                             client_instance.machine_name, raw_data
#                         )
#                         logger.info(
#                             f"[{client_instance.machine_name}] Processed Data: {processed_data}"
#                         )

#                         with data_lock:
#                             latest_machine_data[client_instance.machine_name] = (
#                                 processed_data
#                             )

#                         # --- Store latest status for DB writing ---
#                         with latest_status_for_db_write_lock:
#                             current_timestamp = time.time()
#                             status_text = processed_data.get("Status_Text", "N/A")
#                             spindle_speed = processed_data.get("Spindle_Speed")
#                             feed_rate = processed_data.get("FeedRate_mm_per_min")
#                             current_program = processed_data.get("Current_Program", None)
                            
#                             latest_status_for_db_write[client_instance.machine_name] = {
#                                 "timestamp": current_timestamp,
#                                 "status_text": status_text,
#                                 "spindle_speed": spindle_speed,
#                                 "feed_rate": feed_rate,
#                                 "current_program": current_program 
#                             }
#                             logger.debug(f"[{client_instance.machine_name}] Latest status for DB write updated with program: {current_program}.")
#                         # --- End store latest status for DB writing ---
#                     else:
#                         logger.warning(
#                             f"[{client_instance.machine_name}] No raw data received. `latest_machine_data` not updated."
#                         )
                        
#                         time.sleep(interval)
#             except Exception as e:
#                 logger.critical(
#                     f"[{client_instance.machine_name}] An error occurred during polling/processing: {e}",
#                     exc_info=True,
#                 )
#             finally:
#                 client_instance.disconnect()
#                 logger.info(
#                     f"[{client_instance.machine_name}] Polling stopped and client disconnected."
#                 )

#             break
#         else:
#             logger.warning(
#                 f"[{client_instance.machine_name}] Connection attempt {attempt+1}/{max_retries} failed. Retrying in 5 seconds."
#             )
#             time.sleep(retry_delay)
#     else:
#         logger.error(
#             f"[{client_instance.machine_name}] All connection attempts failed, thread terminating."
#         )
# --- Fungsi untuk Polling Mesin (untuk Thread) ---
def poll_machine_thread_target(client_instance, interval, stop_event):
    """
    Target fungsi untuk setiap thread. Mengelola koneksi dan polling untuk satu mesin.
    
    Revised to handle connection loss and persistent reconnection attempts.
    """
    logger.info(f"[{client_instance.machine_name}] Starting polling thread.")
    while not stop_event.is_set():
        if not client_instance.connected: # Changed from .is_connected()
            logger.info(f"[{client_instance.machine_name}] Attempting to connect...")
            try:
                client_instance.connect()
                if client_instance.connected: # Changed from .is_connected()
                    logger.info(f"[{client_instance.machine_name}] Successfully connected. Starting data polling loop.")
                else:
                    logger.warning(f"[{client_instance.machine_name}] Connection attempt failed. Retrying in {interval} seconds.")
                    stop_event.wait(interval)
                    continue
            except Exception as e:
                logger.error(f"[{client_instance.machine_name}] Connection error: {e}. Retrying in {interval} seconds.", exc_info=True)
                stop_event.wait(interval)
                continue

        # Polling loop
        try:
            raw_data = client_instance.read_all_variables()

            if raw_data is not None:
                logger.info(f"[{client_instance.machine_name}] Raw Data: {raw_data}")

                processed_data = data_processor.process_opcua_data(
                    client_instance.machine_name, raw_data
                )
                logger.info(f"[{client_instance.machine_name}] Processed Data: {processed_data}")

                with data_lock:
                    latest_machine_data[client_instance.machine_name] = processed_data

                # Store latest status for DB writing
                with latest_status_for_db_write_lock:
                    current_timestamp = time.time()
                    status_text = processed_data.get("Status_Text", "N/A")
                    spindle_speed = processed_data.get("Spindle_Speed")
                    feed_rate = processed_data.get("FeedRate_mm_per_min")
                    current_program = processed_data.get("Current_Program", None)
                    
                    latest_status_for_db_write[client_instance.machine_name] = {
                        "timestamp": current_timestamp,
                        "status_text": status_text,
                        "spindle_speed": spindle_speed,
                        "feed_rate": feed_rate,
                        "current_program": current_program 
                    }
                    logger.debug(f"[{client_instance.machine_name}] Latest status for DB write updated with program: {current_program}.")
            else:
                logger.warning(
                    f"[{client_instance.machine_name}] No raw data received. `latest_machine_data` not updated. Disconnecting to force reconnect."
                )
                client_instance.disconnect()
                
            stop_event.wait(interval)

        except Exception as e:
            logger.critical(
                f"[{client_instance.machine_name}] An error occurred during polling/processing: {e}",
                exc_info=True,
            )
            logger.info(f"[{client_instance.machine_name}] Disconnecting to trigger reconnection attempt.")
            client_instance.disconnect()

    # If the stop event is set, we reach here and the thread terminates
    if client_instance.connected: # Changed from .is_connected()
        client_instance.disconnect()
        logger.info(f"[{client_instance.machine_name}] Polling stopped and client disconnected.")
    logger.info(f"[{client_instance.machine_name}] Thread terminated gracefully.")


def json_writer_thread_target(json_filepath, lock, stop_event, data_source):
    """
    Thread target to periodically write aggregated machine data to a JSON file.
    """
    logger.info(f"Starting JSON writer thread, writing to {json_filepath}")
    while not stop_event.is_set():
        try:
            with lock:
                data_to_write = data_source.copy()

            with open(json_filepath, "w") as f:
                json.dump(data_to_write, f, indent=4)
            logger.debug(
                f"Successfully wrote data to {json_filepath}. Data keys: {data_to_write.keys()}"
            )
        except Exception as e:
            logger.error(f"Error writing to JSON file {json_filepath}: {e}")

        stop_event.wait(1)
    logger.info("JSON writer thread stopped.")


def db_writer_status_logs_thread_target(interval, stop_event, latest_status_data_ref, latest_status_data_lock_ref):
    """
    NEW: Thread target to periodically save the latest machine status to the database.
    """
    logger.info(f"Starting DB writer thread for status logs, saving every {interval} seconds.")
    while not stop_event.is_set():
        current_time = datetime.datetime.now()
        table_name = get_status_log_table_name(current_time)
        
        create_status_log_table(table_name)

        with latest_status_data_lock_ref:
            for machine_name, status_info in latest_status_data_ref.items():
                try:
                    program_to_save = status_info.get("current_program", None)
                    
                    save_status_log(
                        machine_name=machine_name,
                        timestamp=status_info["timestamp"],
                        status_text=status_info["status_text"],
                        spindle_speed=status_info["spindle_speed"],
                        feed_rate=status_info["feed_rate"],
                        current_program=program_to_save, 
                        table_name=table_name 
                    )
                    logger.debug(f"[DB-Writer-Status-Logs-Thread] Saved log for {machine_name} at {datetime.datetime.fromtimestamp(status_info['timestamp'])} with program: {program_to_save}")
                except Exception as e:
                    logger.error(f"[DB-Writer-Status-Logs-Thread] Error saving log for {machine_name}: {e}")
            
        stop_event.wait(interval)
    logger.info("DB writer thread for status logs stopped.")


def shift_calculation_thread_target(
    interval,
    stop_event,
    latest_machine_data_ref,
    data_lock_ref,
    machine_shift_metrics_ref,
    shift_metrics_lock_ref,
    shifts_saved_to_db_ref,
    shifts_saved_to_db_lock_ref,
):
    """
    Thread target to periodically calculate shift metrics for all machines.
    Also saves current shift metrics to DB and checks for completed shifts to save to final DB.
    """
    logger.debug("--- Inside shift_calculation_thread_target function. Starting initial checks. ---")
    
    # PERBAIKAN: Memindahkan definisi 'now' ke dalam try-while loop
    # agar selalu didefinisikan dengan scope yang benar di setiap iterasi.
    try: 
        while not stop_event.is_set():
            # DEFINE 'now' DI SINI (SETIAP ITERASI)
            now = datetime.datetime.now(timezone.utc) 
            
            # Mendapatkan nama tabel program report bulanan dan memastikan tabelnya ada
            current_month_program_report_table = get_program_report_table_name(now)
            create_program_report_table_monthly(current_month_program_report_table)

            logger.info("[Shift-Calc-Thread] Performing shift calculations and DB write check...")

            current_shift_name, current_shift_start_utc, current_shift_end_utc = shift_calculator.get_current_shift_info(now)
            prev_shift_name, prev_shift_start_utc, prev_shift_end_utc = shift_calculator.get_previous_shift_info(now)

            shifts_to_calculate = {
                current_shift_name: (current_shift_start_utc, current_shift_end_utc),
                prev_shift_name: (prev_shift_start_utc, prev_shift_end_utc)
            }
            logger.info(f"Calculating shift metrics for shifts: {list(shifts_to_calculate.keys())}")

            with shift_metrics_lock_ref:
                with data_lock_ref:
                    logger.debug(f"DEBUG: Machines being processed in shift calculation: {list(latest_machine_data_ref.keys())}")

                    for machine_name in latest_machine_data_ref.keys():
                        overall_log_start_dt = min(current_shift_start_utc, prev_shift_start_utc)
                        overall_log_end_dt = max(current_shift_end_utc, now) 
                        
                        all_relevant_status_logs = get_status_logs_for_machine(machine_name, overall_log_start_dt, overall_log_end_dt)
                        all_relevant_status_logs.sort(key=lambda x: x['timestamp'])
                        logger.debug(f"Processing shift metrics for machine: {machine_name} with {len(all_relevant_status_logs)} log entries from DB.")
                        
                        if machine_name not in machine_shift_metrics_ref:
                            machine_shift_metrics_ref[machine_name] = {}

                        for shift_name, (shift_start_dt, shift_end_dt) in shifts_to_calculate.items():
                            runtime_sec, idletime_sec = shift_calculator.calculate_runtime_idletime(
                                all_relevant_status_logs, shift_start_dt, shift_end_dt
                            )
                            
                            total_elapsed_time_in_shift_seconds = (min(now, shift_end_dt) - shift_start_dt).total_seconds()
                            total_elapsed_time_in_shift_seconds = max(0.0, total_elapsed_time_in_shift_seconds)
                            accounted_time_seconds = runtime_sec + idletime_sec
                            other_time_sec = max(0.0, total_elapsed_time_in_shift_seconds - accounted_time_seconds)

                            machine_shift_metrics_ref[machine_name][shift_name] = {
                                "runtime_hhmm": shift_calculator.format_seconds_to_hhmm(runtime_sec),
                                "idletime_hhmm": shift_calculator.format_seconds_to_hhmm(idletime_sec),
                                "runtime_seconds": round(runtime_sec, 2),
                                "idletime_seconds": round(idletime_sec, 2),
                                "other_time_seconds": round(other_time_sec, 2),
                                "shift_start": shift_start_dt.isoformat(),
                                "shift_end": shift_end_dt.isoformat(),
                            }
                            logger.debug(
                                f"  Machine {machine_name}, Shift {shift_name}: Runtime={runtime_sec:.2f}s, Idletime={idletime_sec:.2f}s, Other Time={other_time_sec:.2f}s"
                            )

                            current_shift_metrics_table = get_shift_metrics_table_name(shift_start_dt)
                            save_shift_metrics(
                                machine_name=machine_name,
                                shift_name=shift_name,
                                runtime_sec=runtime_sec,
                                idletime_sec=idletime_sec,
                                other_time_sec=other_time_sec,
                                shift_start_time=shift_start_dt,
                                shift_end_time=shift_end_dt,
                                table_name=current_shift_metrics_table
                            )
                            logger.debug(f"[DB-Writer-Shift-Metrics-Realtime] Saved real-time metrics for {machine_name} - {shift_name}")

                # --- Check and save completed shifts to final DB table ---
                logger.debug("[Shift-Calc-Thread] Checking for completed shifts to save to final table.")
                with shifts_saved_to_db_lock_ref:
                    try:
                        messages, _ = check_and_save_completed_shifts(
                            shift_metrics_data=machine_shift_metrics_ref,
                            current_time=now, # Menggunakan 'now' yang baru didefinisikan
                            shifts_saved_to_db_lock=shifts_saved_to_db_lock_ref,
                            shifts_saved_state=shifts_saved_to_db_ref,
                            shift_metrics_lock=shift_metrics_lock_ref
                        )
                        for msg in messages:
                            logger.info(msg)
                        logger.debug("[Shift-Calc-Thread] Finished checking for completed shifts.")
                    except Exception as e:
                        logger.critical(f"[Shift-Calc-Thread] CRITICAL ERROR checking/saving completed shifts: {e}", exc_info=True)

                # --- Proses dan Simpan Laporan Program ke DB ---
                report_start_dt_utc = (now - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) # Menggunakan 'now'
                report_end_dt_utc = now.replace(hour=23, minute=59, second=59, microsecond=999999) # Menggunakan 'now'

                for machine_name in latest_machine_data_ref.keys():
                    logger.debug(f"[Shift-Calc-Thread] Processing program cycles for {machine_name} from {report_start_dt_utc.date()} to {report_end_dt_utc.date()}.")
                    
                    logs_for_program_processing = get_status_logs_for_machine(
                        machine_name,
                        report_start_dt_utc,
                        report_end_dt_utc
                    )

                    if logs_for_program_processing:
                        program_cycles = program_processor.process_program_cycles_from_logs(machine_name, logs_for_program_processing)
                        
                        if program_cycles:
                            with db_write_lock: 
                                save_program_cycles_to_db(program_cycles)
                        else:
                            logger.debug(f"[Shift-Calc-Thread] No complete program cycles detected for {machine_name} in the current period.")
                    else:
                        logger.debug(f"[Shift-Calc-Thread] No status logs available for {machine_name} in the current period for program report processing.")

            stop_event.wait(interval)
            
    except Exception as e: 
        logger.critical(f"[Shift-Calc-Thread] CRITICAL ERROR: {e}", exc_info=True)
    logger.debug("[Shift-Calc-Thread] Exiting thread.")

def main():
    """
    Main function to run the application.
    """
    CONFIG_FILE = "machines_config.json"
    POLLING_INTERVAL_SECONDS = 1
    SHIFT_CALC_INTERVAL_SECONDS = 5
    
    logger.info("--- Starting Multi-Machine OPC UA Client ---")

    all_machine_configs, url, user, password = load_machine_configs(f"app_core/{CONFIG_FILE}")

    if not all_machine_configs:
        logger.error("No machine configurations loaded or found in 'machines' key. Exiting.")
        exit()

    if url is None:
        logger.error("Error: Global URL not found (neither in environment variables nor in 'global_config'). Cannot proceed. Exiting.")
        exit()
    if user is None:
        logger.error("Error: OPC_UA_USER environment variable not set. User is required. Exiting.")
        exit()
    if password is None:
        logger.error("Error: OPC_UA_PASSWORD environment variable not set. Password is required. Exiting.")
        exit()

    # --- PERBAIKAN PENTING: Inisialisasi koneksi DB di sini ---
    logger.info("Attempting to initialize database connection pool...")
    try:
        init_db_pool() # Panggil fungsi ini untuk inisialisasi pool
        logger.info("Successfully initialized database connection pool.")
    except Exception as e:
        logger.critical(f"Failed to initialize database connection pool: {e}", exc_info=True)
        exit()

    logger.info("Initializing database tables...")
    try:
        init_db() # Panggil fungsi ini untuk membuat tabel
        logger.info("All database tables checked/created successfully.")
    except Exception as e:
        logger.critical(f"Error initializing database tables: {e}", exc_info=True)
        exit()
    # --- AKHIR PERBAIKAN ---

    opc_clients = []
    threads = []
    stop_events = []

    for i, config in enumerate(all_machine_configs):
        machine_name = config.get("name", f"Machine {i+1}")

        try:
            url_to_use = config.get("url", url)
            user_to_use = user
            password_to_use = password

            if url_to_use is None:
                logger.warning(f"Skipping machine '{machine_name}' as no URL is specified (global or specific).")
                continue

            stop_event = threading.Event()
            stop_events.append(stop_event)

            client = OpcUaClient(
                url=url_to_use,
                user=user_to_use,
                password=password_to_use,
                variables=config["variables"],
                machine_name=machine_name,
            )
            opc_clients.append(client)

            thread = threading.Thread(
                target=poll_machine_thread_target,
                args=(
                    client,
                    POLLING_INTERVAL_SECONDS,
                    stop_event,
                ),
                name=f"Thread-{machine_name}",
            )
            thread.daemon = True
            threads.append(thread)
        except KeyError as e:
            logger.error(f"Skipping machine '{machine_name}' due to missing configuration key: {e}. Ensure 'variables' is present.")
        except Exception as e:
            logger.error(f"Error creating client for machine '{machine_name}': {e}")

    if not opc_clients:
        logger.error("No valid machine configurations found or clients could be initialized. Exiting.")
        exit()

    json_writer_stop_event_latest_data = threading.Event()
    json_writer_thread_latest_data = threading.Thread(
        target=json_writer_thread_target,
        args=(DATA_FILE, data_lock, json_writer_stop_event_latest_data, latest_machine_data),
        name="JSON-Writer-Latest-Data-Thread",
    )
    json_writer_thread_latest_data.daemon = True
    json_writer_thread_latest_data.start()
    stop_events.append(json_writer_stop_event_latest_data)

    db_writer_status_logs_stop_event = threading.Event()
    db_writer_status_logs_thread = threading.Thread(
        target=db_writer_status_logs_thread_target,
        args=(
            STATUS_LOG_DB_INTERVAL_SECONDS, 
            db_writer_status_logs_stop_event, 
            latest_status_for_db_write, 
            latest_status_for_db_write_lock
            ),
        name="DB-Writer-Status-Logs-Thread"
    )
    db_writer_status_logs_thread.daemon = True
    db_writer_status_logs_thread.start()
    stop_events.append(db_writer_status_logs_stop_event)


    shift_calc_stop_event = threading.Event()
    shift_calc_thread = threading.Thread(
        target=shift_calculation_thread_target,
        args=(
            SHIFT_CALC_INTERVAL_SECONDS,
            shift_calc_stop_event,
            latest_machine_data,
            data_lock,
            machine_shift_metrics,
            shift_metrics_lock,
            shifts_saved_to_db,
            shifts_saved_to_db_lock,
        ),
        name="Shift-Calc-Thread"
    )
    shift_calc_thread.daemon = True
    logger.debug("Attempting to start Shift-Calc-Thread...")
    shift_calc_thread.start()
    stop_events.append(shift_calc_stop_event)

    logger.info(f"\nStarting {len(threads)} machine polling threads...")
    for thread in threads:
        thread.start()

    logger.info("\nAll threads started. Press Ctrl+C to stop the program.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nKeyboardInterrupt detected. Signaling client threads to shut down.")
        for event in stop_events:
            event.set()
        for thread in threads:
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning(f"Thread {thread.name} did not terminate gracefully.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred in the main program: {e}", exc_info=True)
    finally:
        logger.info("Main program finished. All client connections should be closed.")


# --- Main Program ---
if __name__ == "__main__":
   main()