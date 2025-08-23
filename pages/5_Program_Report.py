# pages/5_program_report.py

import streamlit as st
import pandas as pd
import datetime
import sys
import os
import time
from psycopg2 import sql 
import logging

# Pastikan path ke config dan db_manager sudah benar
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app_core.config import (
    DB_CONFIG, 
    MACHINE_DISPLAY_ORDER,
)
from app_core.db_manager import (
    connect_db, 
    format_seconds_to_hhmmss, 
    get_program_report_from_db, 
    get_program_report_table_name, 
    init_db_pool, 
    db_pool, 
    close_db_connection
)

# --- Inisialisasi DB Pool untuk aplikasi Streamlit ini ---
try:
    init_db_pool()
    if db_pool is None:
        st.error("ERROR: Database connection pool failed to initialize. Please check database configuration.")
        st.stop()
except Exception as e:
    logging.critical(f"Critical error during Streamlit DB pool initialization: {e}", exc_info=True)
    st.error(f"Critical error during database connection: {e}")
    st.stop()


# --- Streamlit Page Configuration & CSS ---
st.set_page_config(layout="wide", page_title="Laporan Program Mesin")
st.title("Laporan Program Mesin")

# CSS untuk mengubah ukuran font metrik (opsional, seperti yang Anda tambahkan sebelumnya)
st.markdown(
    """
    <style>
    div[data-testid="stMetricValue"] {
        font-size: 20px;
    }
    div[data-testid="stMetricLabel"] p {
        font-size: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.sidebar.header("Filter Laporan Program")

# --- Dapatkan daftar mesin yang tersedia dari database (dengan caching) ---
@st.cache_data(ttl=3600) # Cache hasil selama 1 jam
def get_available_machines_from_db():
    all_available_machine_names = set()
    conn = None
    cur = None
    try:
        conn = connect_db() 
        if conn is None:
            st.warning("Tidak dapat terhubung ke database untuk memuat daftar mesin.")
            return [] 
        
        cur = conn.cursor()
        
        cur.execute("""
            SELECT DISTINCT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'program_report_%';
        """)
        existing_report_tables = [row[0] for row in cur.fetchall()]

        for table_name in existing_report_tables:
            try:
                cur.execute(sql.SQL("SELECT DISTINCT machine_name FROM {};").format(sql.Identifier(table_name)))
                db_machines = [row[0] for row in cur.fetchall()]
                all_available_machine_names.update(db_machines)
            except Exception as e:
                st.warning(f"Tidak dapat membaca daftar mesin dari tabel laporan '{table_name}': {e}")
                continue
    except Exception as e:
        st.error(f"Error memuat daftar mesin dari DB: {e}")
        return []
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)
    
    ordered_machine_names = [m for m in MACHINE_DISPLAY_ORDER if m in all_available_machine_names]
    ordered_machine_names.extend(sorted([m for m in all_available_machine_names if m not in MACHINE_DISPLAY_ORDER]))
    return ordered_machine_names

ordered_machine_names = get_available_machines_from_db()

if not ordered_machine_names:
    st.warning("Tidak ada data mesin yang ditemukan di tabel laporan program. Pastikan aplikasi utama berjalan dan memproses data program.")
    st.stop()

selected_machine = st.sidebar.selectbox("Pilih Mesin", options=ordered_machine_names)

# Rentang tanggal untuk filter
col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("Tanggal Mulai", value=datetime.date.today() - datetime.timedelta(days=7))
with col2:
    end_date = st.date_input("Tanggal Akhir", value=datetime.date.today())

if start_date > end_date:
    st.sidebar.error("Tanggal mulai tidak boleh lebih lambat dari tanggal akhir.")
    st.stop()

selected_main_program_input = st.sidebar.text_input(
    "Filter Program Induk (masukkan sebagian nama)",
    value=st.session_state.get('main_program_report_filter_input', ""),
    key='main_program_report_filter_input'
).strip()


# --- Load Program Report Data dari DB (dengan caching) ---
@st.cache_data(ttl=60) # Cache data selama 60 detik
def load_and_process_program_report_data(machine_name, start_date, end_date, main_program_filter):
    """
    Memuat laporan program dari DB dan memprosesnya untuk tampilan, 
    mengelompokkan program induk ke dalam sesi-sesi terpisah.
    """
    logs = get_program_report_from_db(machine_name, start_date, end_date)
    
    if not logs:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_report = pd.DataFrame(logs)
    
    if not df_report.empty:
        # Ekstraksi nama induk
        df_report['program_main_name'] = df_report['program_name'].apply(
            lambda x: str(x).split('-')[0].strip() if x and '-' in str(x) else str(x).strip()
        )
        
        # Terapkan filter program induk jika ada
        if main_program_filter:
            df_report = df_report[
                df_report['program_main_name'].str.contains(main_program_filter, case=False, na=False)
            ]
            if df_report.empty:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # PERUBAHAN UTAMA: Deteksi sesi berdasarkan program induk yang berurutan DAN jeda waktu
        df_report = df_report.sort_values(by='start_time').reset_index(drop=True)

        # Cek kapan program induk berubah ATAU ada jeda waktu besar
        df_report['prev_end_time'] = df_report['end_time'].shift(1)
        df_report['time_gap'] = (df_report['start_time'] - df_report['prev_end_time']).dt.total_seconds().fillna(0)

        # Tentukan ambang batas jeda (misalnya, 1 jam = 3600 detik)
        SESSION_GAP_THRESHOLD_SECONDS = 3600 

        # Sesi baru dimulai jika:
        # 1. Nama program induk berubah.
        # 2. Ada jeda waktu antar sub-program yang lebih besar dari ambang batas.
        df_report['session_break'] = (
            (df_report['program_main_name'] != df_report['program_main_name'].shift(1)) |
            (df_report['time_gap'] > SESSION_GAP_THRESHOLD_SECONDS)
        )
        
        df_report['session_id'] = df_report['session_break'].cumsum()

        # Agregasi data untuk setiap sesi
        df_summary_main_program = df_report.groupby(['program_main_name', 'session_id']).agg(
            start_time=('start_time', 'min'),
            end_time=('end_time', 'max'),
            total_processing_seconds=('duration_seconds', 'sum')
        ).reset_index()

        df_summary_main_program['duration_total_seconds'] = (
            df_summary_main_program['end_time'] - df_summary_main_program['start_time']
        ).dt.total_seconds()
        
        df_summary_main_program['loss_time_seconds'] = (
            df_summary_main_program['duration_total_seconds'] - df_summary_main_program['total_processing_seconds']
        )
        
        # Asumsikan Qty = 1 per sesi untuk laporan ini
        df_summary_main_program['quantity'] = 1 
        
        df_summary_main_program['duration_per_piece_seconds'] = df_summary_main_program['duration_total_seconds'] / df_summary_main_program['quantity']
        df_summary_main_program['loss_time_per_piece_seconds'] = df_summary_main_program['loss_time_seconds'] / df_summary_main_program['quantity']
        df_summary_main_program['cutting_time_per_piece_seconds'] = df_summary_main_program['total_processing_seconds'] / df_summary_main_program['quantity']

        # Format semua durasi ke HH:MM:SS
        df_summary_main_program['duration_total'] = df_summary_main_program['duration_total_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['total_processing_time'] = df_summary_main_program['total_processing_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['loss_time'] = df_summary_main_program['loss_time_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['duration_per_piece'] = df_summary_main_program['duration_per_piece_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['loss_time_per_piece'] = df_summary_main_program['loss_time_per_piece_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['cutting_time_per_piece'] = df_summary_main_program['cutting_time_per_piece_seconds'].apply(format_seconds_to_hhmmss)
        
        df_summary_main_program['start_time'] = df_summary_main_program['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_summary_main_program['end_time'] = df_summary_main_program['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        df_summary_main_program = df_summary_main_program.sort_values(by='start_time', ascending=True).drop(columns=['session_id'])

        # Ringkasan data Sub-Program (tetap sama)
        df_summary_subprogram = df_report.groupby('program_name').agg(
            start_time=('start_time', 'min'),
            end_time=('end_time', 'max'),
            total_processing_seconds=('duration_seconds', 'sum')
        ).reset_index()

        df_summary_subprogram['duration_total_seconds'] = (df_summary_subprogram['end_time'] - df_summary_subprogram['start_time']).dt.total_seconds()
        df_summary_subprogram['loss_time_seconds'] = df_summary_subprogram['duration_total_seconds'] - df_summary_subprogram['total_processing_seconds']
        df_summary_subprogram['duration_total'] = df_summary_subprogram['duration_total_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_subprogram['total_processing_time'] = df_summary_subprogram['total_processing_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_subprogram['loss_time'] = df_summary_subprogram['loss_time_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_subprogram['start_time'] = df_summary_subprogram['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_summary_subprogram['end_time'] = df_summary_subprogram['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_summary_subprogram = df_summary_subprogram.sort_values(by='start_time', ascending=True)

        # Detail siklus (untuk tampilan tabel)
        df_report_display = df_report.rename(columns={
            "machine_name": "machine_name",
            "program_name": "program_name",
            "start_time": "start_time",
            "end_time": "end_time"
        })
        df_report_display['duration'] = df_report_display['duration_seconds'].apply(format_seconds_to_hhmmss)
        df_report_display['start_time'] = df_report_display['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_report_display['end_time'] = df_report_display['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_report_display = df_report_display[[
            "machine_name", "program_name", "start_time", "end_time", "duration"
        ]].copy()
        
        return df_summary_main_program, df_summary_subprogram, df_report_display
    
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# --- Main Content ---
df_main_program_summary, df_subprogram_summary, df_program_report_display = load_and_process_program_report_data(
    selected_machine, start_date, end_date, selected_main_program_input
)

st.subheader(f"Laporan Program untuk {selected_machine}")
if selected_main_program_input:
    st.markdown(f"##### Filter Program Induk: **'{selected_main_program_input}'**")

if not df_main_program_summary.empty:
    st.markdown("#### Laporan Program Induk (per Sesi)")
    df_main_program_display = df_main_program_summary[[
        'program_main_name', 'start_time', 'end_time', 'duration_total', 'loss_time', 'total_processing_time', 
        'quantity', 'duration_per_piece', 'loss_time_per_piece', 'cutting_time_per_piece'
    ]].rename(columns={
        'program_main_name': 'Main Program',
        'start_time': 'Start Time',
        'end_time': 'End Time',
        'duration_total': 'Duration',
        'total_processing_time': 'Cutting Time',
        'loss_time': 'Loss Time',
        'quantity': 'Qty',
        'duration_per_piece': 'Duration/pcs',
        'loss_time_per_piece': 'Loss Time/pcs',
        'cutting_time_per_piece': 'Cutting Time/pcs'
    })
    st.dataframe(df_main_program_display, use_container_width=True)
else:
    st.info("Tidak ada data ringkasan program induk yang tersedia untuk mesin ini dalam rentang tanggal yang dipilih.")

st.markdown("---")
st.markdown("#### Laporan Sub-Program")
if not df_subprogram_summary.empty:
    st.dataframe(df_subprogram_summary[[
        'program_name', 'start_time', 'end_time', 'duration_total', 'total_processing_time', 'loss_time'
    ]].rename(columns={
        'program_name': 'Program Name',
        'start_time': 'Start',
        'end_time': 'End',
        'duration_total': 'Total Duration',
        'total_processing_time': 'Actual Duration',
        'loss_time': 'Loss Time'
    }), use_container_width=True)
else:
    st.info("Tidak ada data ringkasan sub-program yang tersedia untuk mesin ini dalam rentang tanggal yang dipilih.")

st.markdown("---")
st.markdown("#### Detail Siklus Sub-Program")
if not df_program_report_display.empty:
    st.dataframe(df_program_report_display, use_container_width=True)
else:
    st.info(f"Tidak ada data siklus program yang tersedia untuk {selected_machine} dalam rentang tanggal yang dipilih.")

# Otomatis refresh halaman setiap 60 detik (sesuaikan sesuai kebutuhan)
time.sleep(60)
st.rerun()