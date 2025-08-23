# app_core/program_processor.py
print(f"EXECUTING PROGRAM PROCESSOR: {__file__}")

import pandas as pd
import datetime
import logging
from datetime import timezone
import os
import sys

# Import konfigurasi dari app_core/config.py
try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
    from app_core.config import RUNNING_STATUSES # Impor status Running yang relevan
except ImportError:
    logging.error("Could not import config.py. Please ensure it's in app_core directory or path is set.")
    RUNNING_STATUSES = []

logger = logging.getLogger(__name__)

def process_program_cycles_from_logs(machine_name: str, logs: list):
    """
    Memproses log status untuk mendeteksi siklus program dan menghitung durasi
    berdasarkan transisi status 'Running' mesin.
    Siklus dimulai ketika status berubah ke 'Running' dan berakhir ketika
    status dari 'Running' berubah ke status lain.
    Nama program untuk siklus diambil dari log pada saat siklus 'Running' dimulai.
    Mengembalikan list of dicts dari siklus program yang terdeteksi,
    siap untuk disimpan ke DB.
    """
    program_cycles_raw = []
    
    if not logs:
        logger.info(f"[{machine_name}] No logs provided for program cycle processing.")
        return program_cycles_raw

    df_log = pd.DataFrame(logs)
    df_log['datetime'] = pd.to_datetime(df_log['timestamp'], unit='s', utc=True)
    df_log = df_log.sort_values(by='datetime').reset_index(drop=True)
    logger.debug(f"[{machine_name}] df_log head after initial processing:\n{df_log.head()}")
    
    # Variabel untuk melacak siklus 'Running' yang sedang aktif
    is_running_cycle_active = False
    current_cycle_start_time = None
    program_name_at_cycle_start = None

    for index, row in df_log.iterrows():
        log_time = row['datetime']
        log_status = row['status_text']
        log_program_raw = row.get('current_program')

    # Iterasi melalui log menggunakan perulangan for standar dengan indeks untuk lookahead/lookback yang lebih mudah
    #for i in range(len(df_log)):
        #row = df_log.iloc[i]
        #log_time = row['datetime']
        #log_status = row['status_text']
        #log_program_raw = row.get('current_program')    
        
        # Normalisasi nama program (handle None, NaN, atau string kosong)
        # Mengubah str(log_program_raw).strip() untuk menghindari 'nan' string.
        if log_program_raw is None or pd.isna(log_program_raw) or (isinstance(log_program_raw, str) and log_program_raw.strip() == ""):
            log_program = "N/A (No Program)"
        else:
            log_program = str(log_program_raw).strip()

        #logger.debug(f"[{machine_name}] Processing log entry: Time={log_time}, Status='{log_status}', Raw Program='{log_program_raw}', Normalized Program='{log_program}'") # Log detail program

        # Deteksi awal siklus 'Running'
        if log_status in RUNNING_STATUSES:
            if not is_running_cycle_active:
                # Transisi dari non-Running ke Running
                current_cycle_start_time = log_time
                program_name_at_cycle_start = log_program # Ambil nama program saat running dimulai
                is_running_cycle_active = True
                logger.debug(f"[{machine_name}] Starting new running cycle at {current_cycle_start_time} with program '{program_name_at_cycle_start}'.")
            # else: already running, just continue the current cycle
        else: # log_status is not a RUNNING_STATUS
            if is_running_cycle_active:
                # Transisi dari Running ke non-Running
                cycle_end_time = log_time
                duration_seconds = (cycle_end_time - current_cycle_start_time).total_seconds()

                # Convert to milliseconds for robust integer comparison
                duration_milliseconds = int(duration_seconds * 1000)

                logger.debug(
                    f"[{machine_name}] ENDING running cycle: "
                    f"Program='{program_name_at_cycle_start}', "
                    f"Start={current_cycle_start_time}, "
                    f"End={cycle_end_time}, "
                    f"Duration={duration_seconds:.3f} seconds. "
                    f"Condition duration_milliseconds > 1: {duration_milliseconds > 1}"
                )

                if duration_milliseconds > 1: # Removed program name filter
                    # --- LOG DEBUG KETIKA SIKLUS BENAR-BENAR TERDETEKSI & DISIMPAN ---
                    logger.debug(
                        f"[{machine_name}] FINISHED cycle: "
                        f"Program='{program_name_at_cycle_start}', "
                        f"Start={current_cycle_start_time}, "
                        f"End={cycle_end_time}, "
                        f"Duration={duration_seconds:.2f} seconds. Adding to raw list."
                    )
                    program_cycles_raw.append({
                    "machine_name": machine_name,
                        "nama_program": program_name_at_cycle_start,
                        "waktu_mulai": current_cycle_start_time,
                        "waktu_selesai": cycle_end_time,
                        "durasi_seconds": duration_seconds,
                    })
                else:
                    # Log jika siklus berakhir tetapi tidak memenuhi syarat untuk disimpan
                    logger.debug(
                        f"[{machine_name}] Running cycle ended but not saved: "
                        f"Program='{program_name_at_cycle_start}', "
                        f"Start={current_cycle_start_time}, "
                        f"End={cycle_end_time}, "
                        f"Duration={duration_seconds:.2f} seconds. (Duration <= 0.001 - Milliseconds: {duration_milliseconds})" # Updated log message
                    )
                
                # Reset state for next cycle, regardless if saved or not
                is_running_cycle_active = False
                current_cycle_start_time = None
                program_name_at_cycle_start = None

    # Tangani siklus terakhir jika masih 'Running' pada akhir data log yang diberikan.
    # Ini berarti mesin masih dalam status 'Running' saat log terakhir dicatat.
    if is_running_cycle_active and current_cycle_start_time is not None:
        last_log_time_in_df = df_log.iloc[-1]['datetime']
        # End time of this cycle is the time of the last log entry in the provided DataFrame
        cycle_end_time = last_log_time_in_df 
        duration_seconds = (cycle_end_time - current_cycle_start_time).total_seconds()
        duration_milliseconds = int(duration_seconds * 1000)

        # NEW DEBUG LOG: Print exact duration for final ongoing cycle
        logger.debug(
            f"[{machine_name}] FINAL (ongoing) cycle check: "
            f"Program='{program_name_at_cycle_start}', "
            f"Start={current_cycle_start_time}, "
            f"End={cycle_end_time}, "
            f"Duration={duration_seconds:.3f} seconds. "
            f"Condition duration_milliseconds > 1: {duration_milliseconds > 1}"
        )

        # MODIFIED CONDITION: Only check for duration > 1 millisecond
        if duration_milliseconds > 1: # Removed program name filter
            # --- LOG DEBUG KETIKA SIKLUS TERAKHIR (SEDANG BERJALAN) DISIMPAN ---
            logger.debug(
                f"[{machine_name}] FINAL (ongoing) cycle: "
                f"Program='{program_name_at_cycle_start}', "
                f"Start={current_cycle_start_time}, "
                f"End={cycle_end_time}, "
                f"Duration={duration_seconds:.2f} seconds. Adding to raw list."
            )
            program_cycles_raw.append({
                "machine_name": machine_name,
                "nama_program": program_name_at_cycle_start,
                "waktu_mulai": current_cycle_start_time,
                "waktu_selesai": cycle_end_time,
                "durasi_seconds": duration_seconds,
            })
        else:
            # Log jika siklus terakhir berakhir tetapi tidak memenuhi syarat untuk disimpan
            logger.debug(
                f"[{machine_name}] Final (ongoing) running cycle NOT SAVED (too short): "
                f"Program='{program_name_at_cycle_start}', "
                f"Start={current_cycle_start_time}, "
                f"End={cycle_end_time}, "
                f"Duration={duration_seconds:.2f} seconds. (Duration <= 0.001 - Milliseconds: {duration_milliseconds})"
            )

    # Log jumlah total siklus yang valid sebelum dikembalikan
    logger.info(f"[{machine_name}] Program processing finished. Total valid cycles for DB: {len(program_cycles_raw)}")
    # Hapus print debug yang tidak perlu ini karena sudah ada logger.debug
    # print(f"DEBUG: program_cycles_raw BEFORE RETURN for {machine_name}: {program_cycles_raw}")  
    return program_cycles_raw