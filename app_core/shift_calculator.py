# app_core/shift_calculator.py
print("EXECUTING: app_core/shift_calculator.py")

import logging
import time
import datetime
from datetime import timezone
import pandas as pd # Digunakan dalam fungsi calculate_runtime_idletime
import threading

# Import fungsi dan konfigurasi dari modul lain di app_core
from .db_manager import (
    get_status_logs_for_machine,
    save_shift_metrics,
    save_program_cycles_to_db, 
    create_program_report_table_monthly, 
    get_program_report_table_name,
    get_shift_metrics_table_name,
    check_and_save_completed_shifts,
    format_seconds_to_hhmm # Mengimpor fungsi format HH:MM dari db_manager
)
from .config import SHIFTS, RUNNING_STATUSES, IDLE_STATUSES, OTHER_STATUSES, SHIFT_CALC_INTERVAL_SECONDS
from .program_processor import process_program_cycles_from_logs # NEW: Import fungsi dari file baru

logger = logging.getLogger(__name__)

# --- Shared State and Locks for Shift Calculation ---
last_status_info = {} # Digunakan untuk melacak status terakhir mesin demi kalkulasi durasi
#program_report_lock = threading.RLock()


# --- Helper Functions for Shift Boundaries and Time Calculation ---

def get_current_shift_info(current_time: datetime.datetime):
    """
    Menentukan shift yang sedang berjalan berdasarkan waktu saat ini (UTC aware).
    Mengembalikan tuple: (shift_name: str, shift_start_utc_aware: datetime.datetime, shift_end_utc_aware: datetime.datetime).
    """
    # Pastikan current_time adalah objek datetime yang timezone-aware
    # Konversi ke waktu lokal untuk pencocokan shift
    try:
        import tzlocal
        local_tz = tzlocal.get_localzone()
    except ImportError:
        logger.warning("tzlocal not found. Assuming Asia/Jakarta timezone for local time conversion.")
        class AsiaJakartaTZ(datetime.tzinfo):
            def utcoffset(self, dt): return datetime.timedelta(hours=7)
            def dst(self, dt): return datetime.timedelta(0)
            def tzname(self, dt): return "WIB"
        local_tz = AsiaJakartaTZ()

    current_time_local_aware = current_time.astimezone(local_tz)

    for shift_name, (start_hour, end_hour) in SHIFTS.items():
        # Buat objek datetime lokal untuk awal shift pada tanggal current_time_local_aware
        shift_start_local = datetime.datetime.combine(current_time_local_aware.date(), datetime.time(start_hour, 0, 0), tzinfo=local_tz)
        
        # Tangani shift yang melintasi tengah malam (misal: 22:00-06:00)
        if end_hour == 0: # Ini berarti shift berakhir pada 00:00 hari berikutnya
            shift_end_local = datetime.datetime.combine(current_time_local_aware.date() + datetime.timedelta(days=1), datetime.time(0, 0, 0), tzinfo=local_tz)
        else: # Shift berakhir pada hari yang sama
            shift_end_local = datetime.datetime.combine(current_time_local_aware.date(), datetime.time(end_hour, 0, 0), tzinfo=local_tz)
        
        # Cek apakah waktu lokal saat ini berada di dalam batas shift ini
        if shift_start_local <= current_time_local_aware < shift_end_local:
            # Konversi kembali ke UTC aware sebelum mengembalikan
            return shift_name, shift_start_local.astimezone(timezone.utc), shift_end_local.astimezone(timezone.utc)

    # Fallback jika tidak ada shift yang terdefinisi mencakup waktu saat ini
    # Ini bisa terjadi jika ada celah antar shift atau waktu di luar jadwal kerja
    logger.warning(f"No active shift found for current time: {current_time_local_aware.isoformat()}. Defaulting to 'Unscheduled'.")
    # Sebagai fallback, kembalikan periode 8 jam di sekitar waktu saat ini
    fallback_start_utc = current_time.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=4)
    fallback_end_utc = current_time.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=4)
    return "Unscheduled", fallback_start_utc, fallback_end_utc


def get_previous_shift_info(current_time: datetime.datetime):
    """
    Menentukan shift sebelumnya berdasarkan waktu saat ini (UTC aware).
    Mengembalikan tuple: (shift_name: str, prev_shift_start_utc_aware: datetime.datetime, prev_shift_end_utc_aware: datetime.datetime).
    """
    # Dapatkan waktu mulai shift yang sedang aktif (dalam UTC)
    _, current_shift_start_utc, _ = get_current_shift_info(current_time)

    # Konversi waktu mulai shift saat ini ke waktu lokal untuk mencari shift sebelumnya
    try:
        import tzlocal
        local_tz = tzlocal.get_localzone()
    except ImportError:
        logger.warning("tzlocal not found. Assuming Asia/Jakarta timezone for local time conversion.")
        class AsiaJakartaTZ(datetime.tzinfo):
            def utcoffset(self, dt): return datetime.timedelta(hours=7)
            def dst(self, dt): return datetime.timedelta(0)
            def tzname(self, dt): return "WIB"
        local_tz = AsiaJakartaTZ()

    current_shift_start_local = current_shift_start_utc.astimezone(local_tz)

    # Dapatkan waktu sesaat sebelum shift saat ini dimulai (misalnya 23:59:59 dari hari sebelumnya)
    time_just_before_current_shift = current_shift_start_local - datetime.timedelta(seconds=1)

    # Loop melalui semua definisi shift untuk menemukan shift yang berakhir tepat sebelum
    # `time_just_before_current_shift` (artinya, `time_just_before_current_shift` berada di dalam shift itu)
    # Kita hanya perlu mengiterasi `SHIFTS` sekali karena kita tahu definisinya statis.
    for shift_name, (start_hour, end_hour) in SHIFTS.items():
        shift_start_candidate_local = datetime.datetime.combine(time_just_before_current_shift.date(), datetime.time(start_hour, 0, 0), tzinfo=local_tz)
        
        # Tangani shift yang melintasi tengah malam
        if end_hour == 0:
            shift_end_candidate_local = datetime.datetime.combine(time_just_before_current_shift.date() + datetime.timedelta(days=1), datetime.time(0, 0, 0), tzinfo=local_tz)
        else:
            shift_end_candidate_local = datetime.datetime.combine(time_just_before_current_shift.date(), datetime.time(end_hour, 0, 0), tzinfo=local_tz)

        if shift_start_candidate_local <= time_just_before_current_shift < shift_end_candidate_local:
            # Jika ditemukan, kembalikan nama shift dan batasnya dalam UTC
            return shift_name, shift_start_candidate_local.astimezone(timezone.utc), shift_end_candidate_local.astimezone(timezone.utc)
    
    # Fallback jika tidak ada definisi shift yang cocok untuk waktu sebelumnya.
    # Ini mungkin terjadi jika aplikasi baru dimulai setelah downtime panjang.
    logger.warning(f"Could not determine previous shift for {current_time.isoformat()}. Defaulting to an 8-hour block before current shift start.")
    fallback_start_utc = current_shift_start_utc - datetime.timedelta(hours=8)
    fallback_end_utc = current_shift_start_utc
    return "Previous_Shift", fallback_start_utc, fallback_end_utc


def calculate_runtime_idletime(status_logs: list, shift_start: datetime.datetime, shift_end: datetime.datetime):
    """
    Menghitung total runtime dan idletime untuk shift tertentu dari log status.
    Logika ini dirancang untuk mengatasi gap dalam log atau log yang tidak dimulai tepat di awal shift.
    Args:
        status_logs (list): List of dicts dari log status mesin.
                            Diasumsikan sudah disortir berdasarkan 'timestamp'.
        shift_start (datetime.datetime): Waktu mulai shift (UTC aware).
        shift_end (datetime.datetime): Waktu berakhir shift (UTC aware).
    Returns:
        tuple: (runtime_seconds: float, idletime_seconds: float)
    """
    total_runtime = 0.0
    total_idletime = 0.0

    # Mempersiapkan log yang relevan:
    # 1. Cari log terakhir sebelum shift_start untuk menentukan status awal
    # 2. Tambahkan semua log yang berada dalam rentang shift
    
    # Filter log agar hanya yang berada dalam rentang yang lebih luas untuk mencari status awal
    # dan juga log dalam shift. Mengasumsikan status_logs sudah diurutkan.
    relevant_logs = []
    
    # Menemukan status terakhir sebelum shift dimulai
    last_log_before_shift = None
    for log in status_logs:
        log_dt = datetime.datetime.fromtimestamp(log['timestamp'], tz=timezone.utc)
        if log_dt < shift_start:
            last_log_before_shift = log
        elif log_dt >= shift_start and log_dt < shift_end:
            relevant_logs.append(log)
        elif log_dt >= shift_end: # Logs sorted, so we can stop if we're past the shift_end
            break

    # Jika ada log sebelum shift, tambahkan sebagai entri sintetis di awal shift
    if last_log_before_shift:
        synthetic_entry = last_log_before_shift.copy()
        synthetic_entry['timestamp'] = shift_start.timestamp()
        
        # Perbarui status_text jika perlu, atau pastikan itu valid untuk kalkulasi
        if synthetic_entry['status_text'] not in (RUNNING_STATUSES + IDLE_STATUSES + OTHER_STATUSES):
            synthetic_entry['status_text'] = "Idle" # Fallback status
            logger.debug(f"Adjusted synthetic entry status to 'Idle' for '{last_log_before_shift['status_text']}' before shift start.")

        relevant_logs.insert(0, synthetic_entry)
        logger.debug(f"Added synthetic entry at {shift_start.isoformat()} with status '{synthetic_entry['status_text']}' from preceding log.")

    # Jika masih tidak ada log yang relevan sama sekali, berarti tidak ada aktivitas tercatat
    if not relevant_logs:
        logger.debug(f"No relevant logs found within or immediately before shift {shift_start.isoformat()} - {shift_end.isoformat()}. Returning 0.0, 0.0.")
        return 0.0, 0.0

    # Urutkan ulang log setelah penambahan sintetis dan filter awal
    relevant_logs.sort(key=lambda x: x['timestamp'])

    # Hapus duplikat timestamp yang persis sama, pertahankan yang paling akhir/terbaru
    # Ini penting jika ada beberapa update status di timestamp yang sama
    unique_relevant_logs = []
    if relevant_logs:
        unique_relevant_logs.append(relevant_logs[0])
        for i in range(1, len(relevant_logs)):
            if relevant_logs[i]['timestamp'] > unique_relevant_logs[-1]['timestamp']:
                unique_relevant_logs.append(relevant_logs[i])
            else: 
                unique_relevant_logs[-1] = relevant_logs[i] # Update dengan entri yang lebih baru jika timestamp sama
    
    logger.debug(f"Unique relevant logs for final calculation: {unique_relevant_logs}")

    # Iterasi melalui log yang sudah difilter dan diurutkan untuk menghitung durasi
    for i in range(len(unique_relevant_logs)):
        current_log_entry = unique_relevant_logs[i]
        current_status = current_log_entry['status_text']
        current_timestamp = current_log_entry['timestamp']

        # Waktu berakhir untuk periode status ini adalah timestamp log berikutnya
        # Atau akhir shift, atau waktu sekarang jika shift masih berlangsung
        if i + 1 < len(unique_relevant_logs):
            next_timestamp = unique_relevant_logs[i+1]['timestamp']
            segment_end_timestamp = min(next_timestamp, shift_end.timestamp())
        else:
            # Ini adalah log terakhir dalam rentang yang relevan
            # Jika shift masih berlangsung, hitung hingga waktu saat ini
            if shift_end > datetime.datetime.now(timezone.utc):
                segment_end_timestamp = datetime.datetime.now(timezone.utc).timestamp()
            else: # Shift sudah selesai, hitung hingga akhir shift
                segment_end_timestamp = shift_end.timestamp()
        
        # Pastikan periode yang dihitung berada dalam batas shift
        # Ini penting jika log pertama jatuh sebelum shift_start (setelah penambahan sintetis)
        segment_start_timestamp = max(current_timestamp, shift_start.timestamp())
        
        duration = segment_end_timestamp - segment_start_timestamp

        if duration > 0:
            if current_status in RUNNING_STATUSES:
                total_runtime += duration
                logger.debug(f"  Adding {duration:.2f}s to runtime for status '{current_status}'")
            elif current_status in IDLE_STATUSES:
                total_idletime += duration
                logger.debug(f"  Adding {duration:.2f}s to idletime for status '{current_status}'")
            else:
                # Status lain (Alarm, Setup, Manual mode, dll.) akan berkontribusi ke 'other_time'
                # Di sini kita masih memasukkannya ke total_idletime, dan kemudian 'other_time'
                # akan dihitung sebagai total_elapsed - (runtime + idletime) di fungsi pemanggil.
                # Ini sedikit membingungkan karena idletime di sini sebenarnya adalah non-running.
                # Namun, karena OTHER_STATUSES digunakan di tempat lain untuk menghitung other_time_seconds,
                # kita harus konsisten.
                # KOREKSI: Lebih baik kategorikan ini sebagai "unaccounted" atau biarkan default ke idletime
                # dan biarkan logika di thread_target yang memisahkannya secara eksplisit.
                total_idletime += duration # Untuk sementara, masukkan ke idletime, yang akan digunakan untuk total accounted time
                logger.debug(f"  Adding {duration:.2f}s to idletime for OTHER status '{current_status}' (will be part of 'Other Time' in final calc)")

    return total_runtime, total_idletime


# --- Fungsi Utama Shift Calculation Thread ---

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
    logger.debug("--- Inside shift_calculation_thread_target function. Starting initial checks. ---")
    
    try:
        while not stop_event.is_set():
            current_time_utc = datetime.datetime.now(timezone.utc)
            
            # Pastikan tabel program_report_YYYY_MM untuk bulan saat ini ada
            current_month_program_report_table = get_program_report_table_name(current_time_utc)
            create_program_report_table_monthly(current_month_program_report_table)

            logger.info("[Shift-Calc-Thread] Performing shift calculations and DB write check...")

            current_shift_name, current_shift_start_utc, current_shift_end_utc = get_current_shift_info(current_time_utc)
            prev_shift_name, prev_shift_start_utc, prev_shift_end_utc = get_previous_shift_info(current_time_utc)

            shifts_to_calculate = {
                current_shift_name: (current_shift_start_utc, current_shift_end_utc),
                prev_shift_name: (prev_shift_start_utc, prev_shift_end_utc)
            }
            logger.info(f"Calculating shift metrics for shifts: {list(shifts_to_calculate.keys())}")

            with shift_metrics_lock_ref:
                with data_lock_ref: # Lock untuk latest_machine_data_ref
                    logger.debug(f"DEBUG: Machines being processed in shift calculation: {list(latest_machine_data_ref.keys())}")

                    for machine_name in latest_machine_data_ref.keys():
                        # Ambil log yang relevan untuk perhitungan shift metrik dan program report
                        overall_log_start_dt = min(current_shift_start_utc, prev_shift_start_utc)
                        overall_log_end_dt = max(current_shift_end_utc, current_time_utc) 
                        
                        all_relevant_status_logs = get_status_logs_for_machine(machine_name, overall_log_start_dt, overall_log_end_dt)
                        all_relevant_status_logs.sort(key=lambda x: x['timestamp']) # Pastikan log terurut

                        logger.debug(f"Processing shift metrics for machine: {machine_name} with {len(all_relevant_status_logs)} log entries from DB.")
                        
                        if machine_name not in machine_shift_metrics_ref:
                            machine_shift_metrics_ref[machine_name] = {}

                        for shift_name, (shift_start_dt, shift_end_dt) in shifts_to_calculate.items():
                            runtime_sec, idletime_sec = calculate_runtime_idletime(
                                all_relevant_status_logs, shift_start_dt, shift_end_dt
                            )
                            
                            # Hitung other_time_seconds dengan lebih akurat dari total waktu yang berlalu
                            total_elapsed_time_in_shift_seconds = (min(current_time_utc, shift_end_dt) - shift_start_dt).total_seconds()
                            total_elapsed_time_in_shift_seconds = max(0.0, total_elapsed_time_in_shift_seconds) # Pastikan tidak negatif
                            accounted_time_seconds = runtime_sec + idletime_sec
                            other_time_sec = max(0.0, total_elapsed_time_in_shift_seconds - accounted_time_seconds)

                            machine_shift_metrics_ref[machine_name][shift_name] = {
                                "runtime_hhmm": format_seconds_to_hhmm(runtime_sec),
                                "idletime_hhmm": format_seconds_to_hhmm(idletime_sec),
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
                with shifts_saved_to_db_lock_ref: # Memastikan akses aman ke shifts_saved_to_db_ref
                    try:
                        messages, _ = check_and_save_completed_shifts(
                            shift_metrics_data=machine_shift_metrics_ref,
                            current_time=current_time_utc, # Gunakan current_time_utc yang akurat
                            shifts_saved_to_db_lock=shifts_saved_to_db_lock_ref,
                            shifts_saved_state=shifts_saved_to_db_ref,
                            shift_metrics_lock=shift_metrics_lock_ref
                        )
                        for msg in messages:
                            logger.info(msg)
                        logger.debug("[Shift-Calc-Thread] Finished checking for completed shifts.")
                    except Exception as e:
                        logger.critical(f"[Shift-Calc-Thread] CRITICAL ERROR checking/saving completed shifts: {e}", exc_info=True)

            stop_event.wait(interval)
            
    except Exception as e:
        logger.critical(f"[Shift-Calc-Thread] CRITICAL ERROR: {e}", exc_info=True)
    logger.debug("[Shift-Calc-Thread] Exiting thread.")

    