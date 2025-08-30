import logging
import datetime
from datetime import timezone
from collections import defaultdict
import psycopg2
import pandas as pd

# Pastikan Anda mengimpor fungsi-fungsi yang diperlukan dari modul Anda
from app_core.db_manager import (
    init_db_pool,
    connect_db,
    close_db_connection,
    get_status_logs_for_machine,
)
from app_core.program_processor import process_program_cycles_from_logs
from app_core.config import DB_CONFIG

from app_core.db_manager import (
    init_db_pool,
    connect_db,
    close_db_connection,
    get_status_logs_for_machine,
    create_program_report_table_monthly,  # Tambahkan ini
)

# Konfigurasi logging untuk skrip ini
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_logs_for_month_by_machine(start_date, end_date):
    """
    Mengambil semua log status dari database untuk setiap mesin dalam rentang tanggal
    dan mengelompokkannya berdasarkan machine_name.
    """
    all_logs = []
    conn = None
    try:
        conn = connect_db()
        cur = conn.cursor()
        
        # Mengambil semua machine_name unik dari tabel status_log_2025_07
        # Asumsi nama tabel untuk bulan Juli adalah 'machine_status_log_2025_07'
        table_name = "machine_status_log_2025_07" 
        
        # Pastikan tabel status log bulan Juli ada
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');")
        table_exists = cur.fetchone()[0]
        if not table_exists:
            logger.error(f"Tabel {table_name} tidak ditemukan. Tidak dapat melanjutkan.")
            return {}

        cur.execute(f"SELECT DISTINCT machine_name FROM {table_name};")
        machines = [row[0] for row in cur.fetchall()]
        
        logs_by_machine = defaultdict(list)
        for machine_name in machines:
            logs = get_status_logs_for_machine(machine_name, start_date, end_date)
            logs_by_machine[machine_name].extend(logs)
            
        cur.close()
        return logs_by_machine

    except psycopg2.Error as e:
        logger.error(f"Database error saat mengambil log: {e}")
        return {}
    finally:
        if conn:
            close_db_connection(conn)

def save_program_cycles_to_db_for_repair(cycles_data):
    """Menyimpan daftar siklus program ke DB, menggunakan transaksi."""
    conn = None
    try:
        conn = connect_db()
        cur = conn.cursor()
        
        table_name_for_july = "program_report_2025_07"
        
        if not create_program_report_table_monthly(table_name_for_july):
            logger.error(f"Gagal memastikan tabel '{table_name_for_july}' ada. Menghentikan proses.")
            return False

        logger.info(f"Menghapus data lama dari tabel {table_name_for_july}...")
        cur.execute(f"TRUNCATE TABLE {table_name_for_july} RESTART IDENTITY;")
        
        if cycles_data:
            logger.info(f"Menyisipkan {len(cycles_data)} siklus program yang baru...")
            
            # PERBAIKAN: Tambahkan 'report_date' ke dalam daftar kolom
            insert_query = f"""
                INSERT INTO {table_name_for_july} (machine_name, program_name, start_time, end_time, duration_seconds, report_date)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            
            # PERBAIKAN: Tambahkan nilai 'report_date' ke setiap tuple data
            data_to_insert = []
            for cycle in cycles_data:
                # Ambil tanggal dari 'waktu_mulai' untuk digunakan sebagai report_date
                report_date_value = cycle['waktu_mulai'].date()
                data_to_insert.append(
                    (
                        cycle['machine_name'], 
                        cycle['nama_program'], # Menggunakan 'nama_program' karena itu kunci dari `process_program_cycles_from_logs`
                        cycle['waktu_mulai'], 
                        cycle['waktu_selesai'], 
                        int(cycle['durasi_seconds']), # Pastikan durasi diubah menjadi integer
                        report_date_value # Nilai report_date yang baru
                    )
                )
            
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            logger.info("Penyimpanan data baru berhasil.")
            return True
        else:
            logger.info("Tidak ada siklus program yang terdeteksi. Tabel telah dibersihkan.")
            return True

    except psycopg2.Error as e:
        logger.error(f"Database error saat menyimpan data: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            close_db_connection(conn)

def main_repair():
    """Fungsi utama untuk menjalankan perbaikan database."""
    logger.info("--- Memulai skrip perbaikan database untuk data Juli ---")
    
    # Inisialisasi pool koneksi database
    try:
        init_db_pool()
    except Exception as e:
        logger.critical(f"Gagal menginisialisasi pool koneksi database: {e}")
        return

    # Tentukan tanggal mulai dan akhir untuk bulan Juli 2025
    start_of_july = datetime.datetime(2025, 7, 1, tzinfo=timezone.utc)
    end_of_july = datetime.datetime(2025, 8, 1, tzinfo=timezone.utc)
    
    logger.info(f"Mengambil log status dari database dari {start_of_july} sampai {end_of_july}...")
    
    # Langkah 1: Ambil semua log status yang relevan
    logs_by_machine = get_logs_for_month_by_machine(start_of_july, end_of_july)
    
    if not logs_by_machine:
        logger.warning("Tidak ada log yang ditemukan untuk bulan Juli. Selesai.")
        return
        
    all_repaired_cycles = []
    
    # Langkah 2: Proses log untuk setiap mesin
    for machine_name, logs in logs_by_machine.items():
        logger.info(f"Memproses {len(logs)} log untuk mesin: {machine_name}")
        
        # Panggil fungsi pemrosesan yang telah diperbaiki
        processed_cycles = process_program_cycles_from_logs(machine_name, logs)
        all_repaired_cycles.extend(processed_cycles)
        
    logger.info(f"Total siklus program yang terdeteksi dan diperbaiki: {len(all_repaired_cycles)}")
    
    # Langkah 3 & 4: Hapus data lama dan simpan data baru ke database
    logger.info("Menghapus data lama dan menyimpan data yang diperbaiki ke database...")
    save_program_cycles_to_db_for_repair(all_repaired_cycles)
    
    logger.info("--- Proses perbaikan database selesai. ---")

if __name__ == "__main__":
    main_repair()