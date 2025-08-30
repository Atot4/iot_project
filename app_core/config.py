# app_core/config.py
print(f"LOADING CONFIG: {__file__}")


import os
import json
import logging

APP_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
MACHINES_CONFIG_FILE = os.path.join(APP_CORE_DIR, "machines_config.json")
LOG_FILE_PATH = "app_logs/application.log"

try:
    with open(MACHINES_CONFIG_FILE, 'r') as f:
        machines_data = json.load(f)
        MACHINE_CONFIGS_FOR_PROGRAM_REPORT = machines_data.get("machines", [])
except FileNotFoundError:
    logging.error(f"Error: Configuration file '{MACHINES_CONFIG_FILE}' not found. Please ensure the file exists in app_core directory.")
    MACHINE_CONFIGS_FOR_PROGRAM_REPORT = []
except json.JSONDecodeError:
    logging.error(f"Error: Could not decode JSON from '{MACHINES_CONFIG_FILE}'. Please check file format.")
    MACHINE_CONFIGS_FOR_PROGRAM_REPORT = []
except Exception as e:
    logging.error(f"An unexpected error occurred while loading machines_config.json: {e}")
    MACHINE_CONFIGS_FOR_PROGRAM_REPORT = []


# Definisi Shift (jam mulai, jam selesai)
# Ini harus konsisten di seluruh aplikasi.
# Format: {nama_shift: (jam_mulai, jam_selesai)}
# Catatan: Untuk shift yang melewati tengah malam, jam_selesai < jam_mulai (misal, shift_2 16:00-00:00, shift_3 00:00-08:00)
SHIFTS = {
    "shift_1": (8, 16),  # 08:00 - 16:00
    "shift_2": (16, 0),  # 16:00 - 00:00 (tengah malam hari ini)
    "shift_3": (0, 8),   # 00:00 - 08:00 (tengah malam hingga 8 pagi hari ini)
}

# Lokasi file data
DATA_FILE = "machine_data.json"
# SHIFT_METRICS_FILE = "shift_metrics_data.json" # Dihapus, sekarang disimpan ke DB
# STATUS_LOGS_FILE = "machine_status_logs.json" # Dihapus, sekarang disimpan ke DB

# Status categories for runtime/idletime calculation
# Kategori status untuk perhitungan runtime/idletime
RUNNING_STATUSES = ["Running", "Operating", "Processing", "Cycle Start", "Active"]

IDLE_STATUSES = [
    "Idle",
    "Ready",
    "Standby",
    "Program End",
    "Manual mode",
    "Power On",
    "M-Code Stop",
    "Program Stop",
    "Emergency Stop",
    "Fault",
    "NC Reset",
    "Emergency",
    "With Synchronization",
    "Waiting",
    "Stop",
    "Hold",
    "Disconnected",
    "Connected but not sending data",
    "Interrupted",
    "Faulted",
    "Alarm",
    "Unknown/Offline",
    "Undefined Status",
    "N/A",
    "MDI",
    "Setup",
    "Cooling", 
    "Tool Change"
]

OTHER_STATUSES = [
    "Error", 
    "Maintenance",
    "Testing",
    "Paused",
    "Suspended",
    "Warmup",
    "Dry Run",
    "Alarm", 
    "Undefined Status", 
    "N/A", 
]

STATUS_LOG_RETENTION_HOURS = 24
STATUS_LOG_DB_INTERVAL_SECONDS = 10
SHIFT_CALC_INTERVAL_SECONDS = 10
PROGRAM_REPORT_INTERVAL_SECONDS = 10
POLLING_INTERVAL_SECONDS = 1

MACHINE_DISPLAY_ORDER = [
    "Makino V77 - 1000",
    "Makino V33 - 1012",
    "Makino F5(1) - 1008",
    "Makino F5(2) - 1009",
    "Yasda 1 - 1013",
    "Yasda 2 - 1014",
    "Yasda 3 - 1001",
    "OKK - 1015",
    "Mitsui Seiki - 1002",
    "HSM800 - 1011",
    "HPM600 - 1010",
    "HPM800 - 1003",
    "P500 - 1004",
    "Wele 3 - 1007",
    "Wele 4 - 1006",
    "Quaser 4 - 1005" 
]

# --- Konfigurasi Database PostgreSQL ---
DB_CONFIG = {
    "host": "localhost",  
    "database": "iot_db",
    "user": "postgres",
    "password": "peyek#376",
    "port": "5432",  
}

# Nama tabel untuk menyimpan metrik shift yang telah selesai
FINAL_SHIFT_METRICS_TABLE = "final_shift_metrics"

# Prefix untuk nama tabel log status dan metrik shift real-time (untuk tabel dinamis)
STATUS_LOG_TABLE_PREFIX = "machine_status_log_"
SHIFT_METRICS_TABLE_PREFIX = "shift_metrics_"
