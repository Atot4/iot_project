# pages/3_Machine_Timeline.py

import streamlit as st
import pandas as pd
import datetime
import sys
import time
import plotly.express as px
import psycopg2 # Import psycopg2 untuk koneksi database
import os # Pastikan modul 'os' diimpor untuk operasi path

# Pastikan path ke config dan db_manager sudah benar
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app_core.config import (
    DB_CONFIG, # Import konfigurasi database
    MACHINE_DISPLAY_ORDER,
)
# Import fungsi dari db_manager yang diperlukan
from app_core.db_manager import (
    connect_db, # Untuk mendapatkan koneksi DB di dashboard
    get_status_logs_for_machine, # Fungsi utama untuk mengambil log status dari DB
    get_status_log_table_name # Masih berguna untuk debugging atau referensi nama tabel
)

# --- Helper Functions ---

@st.cache_data(ttl=60) # Cache data selama 60 detik untuk performa
def load_status_logs_from_db(machine_name: str, start_date: datetime.date, end_date: datetime.date):
    """
    Memuat data log status dari database PostgreSQL untuk mesin dan rentang tanggal tertentu.
    Menggunakan fungsi get_status_logs_for_machine dari db_manager.
    Mengembalikan list dictionary log atau list kosong jika tidak ditemukan/error.
    """
    # Konversi tanggal ke datetime objek dengan timezone (UTC disarankan untuk konsistensi DB)
    start_dt_utc = datetime.datetime.combine(start_date, datetime.time.min).astimezone(datetime.timezone.utc)
    end_dt_utc = datetime.datetime.combine(end_date, datetime.time.max).astimezone(datetime.timezone.utc)
    
    # Panggil fungsi dari db_manager untuk mengambil log
    logs = get_status_logs_for_machine(machine_name, start_dt_utc, end_dt_utc)
    
    if not logs:
        st.info(f"Tidak ada log status yang ditemukan untuk {machine_name} dari {start_date} hingga {end_date}.")
    
    return logs


# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Machine Status Change Timeline")
st.title("Machine Timeline")

# --- Sidebar for Filters ---
st.sidebar.header("Filter Timeline")

# Untuk mendapatkan daftar mesin yang tersedia, kita bisa kueri DB
# atau mengandalkan MACHINE_DISPLAY_ORDER dan memuat data jika ada.
# Untuk kesederhanaan awal, kita akan kueri distinct machine_name dari DB.
all_available_machine_names = set()
conn = None
cur = None
try:
    conn = connect_db() # Gunakan connect_db dari db_manager
    if conn:
        cur = conn.cursor()
        # Kueri semua tabel machine_status_log_YYYY_MM yang ada
        cur.execute("""
            SELECT DISTINCT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'machine_status_log_%';
        """)
        existing_log_tables = [row[0] for row in cur.fetchall()]

        for table_name in existing_log_tables:
            try:
                cur.execute(f"SELECT DISTINCT machine_name FROM {table_name};")
                db_machines = [row[0] for row in cur.fetchall()]
                all_available_machine_names.update(db_machines)
            except psycopg2.Error as e: # Tangani error jika tabel tidak bisa diakses
                st.warning(f"Tidak dapat membaca daftar mesin dari tabel '{table_name}': {e}")
                continue
except psycopg2.Error as e: # Tangani error koneksi DB
    st.error(f"Error memuat daftar mesin dari DB: {e}")
finally:
    if cur: cur.close()
    if conn: conn.close()

ordered_available_machines = []
# Urutkan berdasarkan MACHINE_DISPLAY_ORDER terlebih dahulu
for machine_name in MACHINE_DISPLAY_ORDER:
    if machine_name in all_available_machine_names:
        ordered_available_machines.append(machine_name)

# Tambahkan mesin yang tidak ada di MACHINE_DISPLAY_ORDER ke akhir, diurutkan alfabetis
remaining_machines = sorted(
    [name for name in all_available_machine_names if name not in MACHINE_DISPLAY_ORDER]
)
ordered_available_machines.extend(remaining_machines)

if not ordered_available_machines:
    st.warning("Tidak ada data mesin dalam database untuk ditampilkan. Pastikan `main_app.py` berjalan dan menyimpan log status ke database.")
else:
    selected_machine = st.sidebar.selectbox(
        "Pilih Mesin",
        options=ordered_available_machines
    )

    # Filter berdasarkan tanggal
    st.sidebar.subheader("Filter Tanggal")
    today = datetime.date.today()
    default_start_date = today - datetime.timedelta(days=1) # Default 1 hari ke belakang
    selected_start_date = st.sidebar.date_input("Tanggal Mulai", value=default_start_date)
    selected_end_date = st.sidebar.date_input("Tanggal Akhir", value=today)

    if selected_start_date > selected_end_date:
        st.sidebar.error("Tanggal mulai tidak boleh lebih lambat dari tanggal akhir.")
    else:
        # --- Proses Data untuk Timeline ---
        # Muat data log status dari database
        machine_log = load_status_logs_from_db(selected_machine, selected_start_date, selected_end_date)
        
        df_log = pd.DataFrame(machine_log)
        
        if not df_log.empty:
            df_log['datetime'] = pd.to_datetime(df_log['timestamp'], unit='s', utc=True) # Pastikan UTC
            
            # Filter berdasarkan rentang tanggal yang dipilih (sudah dilakukan di DB query, tapi jaga-jaga)
            df_filtered = df_log[
                (df_log['datetime'].dt.date >= selected_start_date) &
                (df_log['datetime'].dt.date <= selected_end_date)
            ].copy() # Gunakan .copy() untuk menghindari SettingWithCopyWarning

            if not df_filtered.empty:
                st.subheader(f"Machine Status Timeline for {selected_machine} ({selected_start_date.strftime('%Y-%m-%d')} - {selected_end_date.strftime('%Y-%m-%d')})")
                
                # Urutkan berdasarkan waktu
                df_filtered = df_filtered.sort_values(by='datetime')

                # Deteksi perubahan status untuk timeline
                df_filtered['prev_status'] = df_filtered['status_text'].shift(1)
                
                # Hanya ambil baris di mana status_text berubah atau itu adalah baris pertama
                # Pastikan index.min() digunakan dengan benar
                status_changes = df_filtered[
                    (df_filtered['status_text'] != df_filtered['prev_status']) | (df_filtered.index == df_filtered.index.min())
                ].copy() # Gunakan .copy()

                # --- Siapkan data untuk grafik timeline ---
                chart_data = []
                if not status_changes.empty:
                    for i in range(len(status_changes)):
                        current_row = status_changes.iloc[i]
                        start_dt = current_row['datetime']
                        status = current_row['status_text']
                        spindle_speed = current_row.get('spindle_speed', 'N/A')
                        feed_rate = current_row.get('feed_rate', 'N/A')

                        if i + 1 < len(status_changes):
                            end_dt = status_changes.iloc[i+1]['datetime']
                        else:
                            # Jika ini baris terakhir dan tanggal akhir adalah hari ini, maka end_dt adalah waktu sekarang
                            if selected_end_date == today:
                                end_dt = datetime.datetime.now(datetime.timezone.utc) # Pastikan timezone-aware
                            else:
                                # Jika tidak, end_dt adalah akhir hari yang dipilih
                                end_dt = datetime.datetime.combine(selected_end_date, datetime.time(23, 59, 59)).astimezone(datetime.timezone.utc)
                        
                        # Pastikan end_dt tidak lebih awal dari start_dt (bisa terjadi pada data yang sangat jarang)
                        if end_dt < start_dt:
                            end_dt = start_dt

                        chart_data.append({
                            "Status": status,
                            "Start": start_dt,
                            "End": end_dt,
                            "Mesin": selected_machine,
                            "Kecepatan Spindle": spindle_speed,
                            "Laju Feed": feed_rate
                        })

                if chart_data:
                    df_chart = pd.DataFrame(chart_data)
                    df_chart['Start'] = pd.to_datetime(df_chart['Start'])
                    df_chart['End'] = pd.to_datetime(df_chart['End'])

                    status_colors = {
                        "Running": "#28A745",       # Hijau
                        "Operating": "#28A745",
                        "Processing": "#28A745",
                        "Cycle Start": "#28A745",
                        "Active": "#28A745",
                        "Idle": "#FFC107",          # Oranye
                        "Ready": "#FFC107",
                        "Standby": "#FFC107",
                        "Program End": "#FFC107",
                        "Manual mode": "#FFC107",
                        "Tool Change": "#FFC107",
                        "Power On": "#FFC107",
                        "MDI": "#FFC107",
                        "Memory": "#FFC107",
                        "Edit": "#FFC107",
                        "Handle": "#FFC107",
                        "JOG": "#FFC107",
                        "Teach in JOG": "#FFC107",
                        "Teach in Handle": "#FFC107",
                        "INC·feed": "#FFC107",
                        "Reference": "#FFC107",
                        "TEST": "#FFC107",
                        "Setup": "#FFC107",
                        "Cooling": "#FFC107",
                        "Disconnected": "#DC3545",  # Merah
                        "Emergency Stop": "#DC3545",
                        "Fault": "#DC3545",
                        "Interrupted": "#DC3545",
                        "Faulted": "#DC3545",
                        "Alarm": "#DC3545",
                        "Undefined Status": "#6C757D", # Abu-abu
                        "NC Reset": "#6C757D",
                        "Emergency": "#DC3545", # Menggunakan merah untuk emergency
                        "With Synchronization": "#6C757D",
                        "Waiting": "#FFC107",
                        "Stop": "#6C757D",
                        "Hold": "#6C757D",
                        "Connected but not sending data": "#6C757D",
                        "Unknown/Offline": "#6C757D",
                        "N/A": "#6C757D",
                        "****": "#6C757D",
                    }

                    fig = px.timeline(
                        df_chart,
                        x_start="Start",
                        x_end="End",
                        y="Mesin",
                        color="Status",
                        color_discrete_map=status_colors,
                        title=f"Machine Status Timeline for {selected_machine}",
                        labels={"Start": "Waktu Mulai", "End": "Waktu Akhir", "Status": "Status Mesin"},
                        hover_data={
                            "Kecepatan Spindle": True,
                            "Laju Feed": True,
                            "Start": "|%Y-%m-%d %H:%M:%S", # Format hover datetime
                            "End": "|%Y-%m-%d %H:%M:%S",   # Format hover datetime
                            "Mesin": False
                        },
                        height=300
                    )
                    fig.update_yaxes(autorange="reversed")
                    fig.update_layout(hovermode="x unified")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Tidak ada data yang cukup untuk membuat grafik timeline.")
            else:
                st.info("Tidak ada data log yang tersedia untuk mesin ini dalam rentang tanggal yang dipilih.")
        else:
            st.info("Tidak ada data log yang tersedia untuk mesin ini.")

# Otomatis refresh halaman setiap 30 detik (sesuaikan sesuai kebutuhan)
time.sleep(30)
st.rerun()
