# app_core/program_report_thread.py
import logging
import time
import datetime
import threading # Pastikan ini diimpor
from datetime import timezone
from app_core.db_manager import (
    get_program_report_table_name,
    create_program_report_table_monthly,
    get_status_logs_for_machine,
    save_program_cycles_to_db,
)
from app_core.config import PROGRAM_REPORT_INTERVAL_SECONDS, RUNNING_STATUSES, IDLE_STATUSES, OTHER_STATUSES
from app_core.program_processor import process_program_cycles_from_logs

logger = logging.getLogger(__name__)

# Cache untuk tabel yang sudah dipastikan ada
_verified_program_report_tables = set()
_table_verification_lock = threading.Lock() # Lock baru untuk melindungi set ini

def program_report_thread_target(
        interval: int, 
        stop_event: threading.Event, 
        latest_machine_data_ref: dict, 
        data_lock_ref: threading.RLock
):
    logger.info("[Program-Report-Thread] Starting Program Report thread.")
    try:
        #initial_table_name = get_program_report_table_name(datetime.datetime.now(timezone.utc))
        #logger.debug("[Program-Report-Thread] Attempting to create/verify program report table.")
        #if not create_program_report_table_monthly(initial_table_name):
            #logger.error("[Program-Report-Thread] Failed to create or verify program report table. Exiting.")
            #return
        
        #logger.debug(f"[Program-Report-Thread] Program report table '{initial_table_name}' verified/created successfully.")

        while not stop_event.is_set():
            logger.debug("[Program-Report-Thread] Starting new cycle for program report generation.")
            current_time = datetime.datetime.now(timezone.utc)

            # --- Logika penentuan rentang waktu untuk laporan program ---
            # Menggunakan rentang 24 jam terakhir dari sekarang untuk lebih fleksibel
            report_start_dt_utc = current_time - datetime.timedelta(days=1)
            report_start_dt_utc = report_start_dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            report_end_dt_utc = current_time.replace(hour=23, minute=59, second=59, microsecond=999999)
            # --- Akhir logika rentang waktu ---     

            # Periksa dan buat tabel untuk bulan yang relevan dalam rentang laporan
            month_of_report = report_start_dt_utc.strftime('%Y_%m')
            table_name_for_report = get_program_report_table_name(report_start_dt_utc)

            with _table_verification_lock:
                if table_name_for_report not in _verified_program_report_tables:
                    logger.debug(f"[Program-Report-Thread] Verifying/creating table '{table_name_for_report}' for program reports.")
                    if create_program_report_table_monthly(table_name_for_report):
                        _verified_program_report_tables.add(table_name_for_report)
                        logger.debug(f"[Program-Report-Thread] Table '{table_name_for_report}' verified/created successfully by this thread.")
                    else:
                        logger.error(f"[Program-Report-Thread] Failed to create or verify program report table '{table_name_for_report}'. Skipping report generation for this cycle.")
                        stop_event.wait(interval)
                        continue
                    
            machines_to_process = []
            try:
                with data_lock_ref: # Menggunakan lock untuk mengakses latest_machine_data_ref dengan aman
                    machines_to_process = list(latest_machine_data_ref.keys())
                logger.debug(f"[Program-Report-Thread] Found {len(machines_to_process)} machines to process: {machines_to_process}")
            except Exception as e:
                logger.error(f"[Program-Report-Thread] Error accessing latest_machine_data_ref: {e}", exc_info=True)
                # Lanjutkan saja jika tidak dapat mengambil daftar mesin, coba lagi di siklus berikutnya
                stop_event.wait(interval)
                continue

            if not machines_to_process:
                logger.debug("[Program-Report-Thread] No machines found to process program reports. Waiting.")
                stop_event.wait(interval)
                continue

            # 21/7/25: Tambahkan pemeriksaan stop_event di sini
            if stop_event.is_set():
                logger.info("[Program-Report-Thread] Stop event detected before processing machines. Exiting loop.")
                break

            for machine_name in machines_to_process:
                if stop_event.is_set():
                    logger.info("[Program-Report-Thread] Stop event detected during machine processing. Exiting loop.")
                    break

                logger.debug(f"[Program-Report-Thread] Fetching status logs for {machine_name} from {report_start_dt_utc} to {report_end_dt_utc} for program report.")

                try:
                    logs_for_program_processing = get_status_logs_for_machine(
                        machine_name,
                        report_start_dt_utc,
                        report_end_dt_utc
                    )
                    logger.debug(f"[Program-Report-Thread] Fetched {len(logs_for_program_processing)} status logs for {machine_name}.")

                    if stop_event.is_set():
                        logger.info(f"[Program-Report-Thread] Stop event detected after fetching logs for {machine_name}. Skipping processing.")
                        break

                    if logs_for_program_processing:
                        logger.debug(f"[{machine_name}] Starting program cycle processing with {len(logs_for_program_processing)} logs.")
                        program_cycles = process_program_cycles_from_logs(machine_name, logs_for_program_processing)

                        if program_cycles:
                            logger.info(f"[{machine_name}] Detected {len(program_cycles)} new program cycles. Attempting to save to DB.")
                            if stop_event.is_set():
                                logger.info(f"[{machine_name}] Stop event detected before saving program cycles. Skipping save.")
                                break
                            
                            # Pastikan save_program_cycles_to_db juga memiliki penanganan error internal
                            if save_program_cycles_to_db(program_cycles):
                                logger.info(f"[{machine_name}] Successfully saved {len(program_cycles)} program cycles to DB.")
                            else:
                                logger.warning(f"[{machine_name}] Failed to save program cycles to DB for {machine_name}.")
                        else:
                            logger.debug(f"[{machine_name}] No new complete program cycles detected in the current period for {machine_name}.")
                    else:
                        logger.debug(f"[{machine-name}] No status logs available for program report processing for {machine_name} in the current period.")
                except Exception as e:
                    logger.error(f"[Program-Report-Thread] An error occurred during program report processing for {machine_name}: {e}", exc_info=True)
                    # Jangan menghentikan thread, lanjutkan ke mesin berikutnya atau siklus berikutnya
                    continue 

            logger.debug(f"[Program-Report-Thread] Finished one full iteration of program report generation for all machines. Sleeping for {interval} seconds.")
            stop_event.wait(interval)
    except Exception as e:
        logger.critical(f"[Program-Report-Thread] CRITICAL ERROR in main loop of Program Report Thread: {e}", exc_info=True)
    logger.info("[Program-Report-Thread] Exiting Program Report thread.")