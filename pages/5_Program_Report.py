# pages/5_Machine_Cycle_Report.py

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
st.set_page_config(layout="wide", page_title="Program Report")
st.title("Program Report")

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

#st.sidebar.header("Filter Laporan Program")

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

selected_machine = st.sidebar.selectbox("Select a Machine", options=ordered_machine_names)

# Rentang tanggal untuk filter
col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=datetime.date.today() - datetime.timedelta(days=7))
with col2:
    end_date = st.date_input("End Date", value=datetime.date.today())

if start_date > end_date:
    st.sidebar.error("Tanggal mulai tidak boleh lebih lambat dari tanggal akhir.")
    st.stop()

selected_main_program_input = st.sidebar.text_input(
    "Main Program",
    value=st.session_state.get('main_program_report_filter_input', ""),
    key='main_program_report_filter_input'
).strip().upper()


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
        #Ekstraksi nama induk
        df_report['program_main_name'] = df_report['program_name'].apply(
            lambda x: str(x).split('-')[0].strip() if x and '-' in str(x) else str(x).strip()
        )

        # Tambahkan filter untuk mengabaikan program yang tidak diawali dengan 'N'
        df_report = df_report[df_report['program_main_name'].str.startswith('N', na=False)]


        # Terapkan filter program induk jika ada
        if main_program_filter:
            # Mengidentifikasi waktu mulai dan berakhir dari status 'Running' untuk program induk yang difilter
            running_programs = df_report[
                (df_report['program_main_name'].str.contains(main_program_filter, case=False, na=False))
            ]

            if running_programs.empty:
                # Jika tidak ada status 'Running' untuk program yang difilter, kembalikan DataFrame kosong
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

            start_timestamp_of_running = running_programs['start_time'].min()
            end_timestamp_of_running = running_programs['end_time'].max()

            # Filter seluruh DataFrame berdasarkan rentang waktu 'Running' yang teridentifikasi
            # dan juga filter berdasarkan machine_name (sesuai logika SQL)
            df_report = df_report[
                (df_report['start_time'] >= start_timestamp_of_running) &
                (df_report['end_time'] <= end_timestamp_of_running) &
                (df_report['machine_name'] == selected_machine)
            ]

            if df_report.empty:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # PERUBAHAN UTAMA: Deteksi sesi berdasarkan program induk yang berurutan
        df_report = df_report.sort_values(by='start_time').reset_index(drop=True)

        # Sesi baru dimulai hanya jika nama program induk berubah.
        df_report['session_break'] = (df_report['program_main_name'] != df_report['program_main_name'].shift(1))
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
        
        # Format semua durasi ke HH:MM:SS
        df_summary_main_program['duration_total'] = df_summary_main_program['duration_total_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['total_processing_time'] = df_summary_main_program['total_processing_seconds'].apply(format_seconds_to_hhmmss)
        df_summary_main_program['loss_time'] = df_summary_main_program['loss_time_seconds'].apply(format_seconds_to_hhmmss)
        
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

st.subheader(f"{selected_machine}")

# Use a single markdown line to handle both cases for better readability.
if selected_main_program_input:
    st.markdown(f"##### Main Program **'{selected_main_program_input}'**")
else:
    st.markdown("#### Main Program")

# Check if the main program summary DataFrame is not empty
if not df_main_program_summary.empty:
    # Start with a copy of the full DataFrame
    filtered_main_program_df = df_main_program_summary.copy()

    # If there's a filter, apply it to the DataFrame
    if selected_main_program_input:
        filtered_main_program_df = filtered_main_program_df[
            filtered_main_program_df['program_main_name'].str.contains(
                selected_main_program_input, case=False, na=False
            )
        ]

    # Sort the filtered DataFrame. This happens regardless of whether it was filtered or not.
    filtered_main_program_df = filtered_main_program_df.sort_values(by='start_time', ascending=False)
    
    # Check if the filtered/sorted DataFrame is empty
    if filtered_main_program_df.empty:
        st.info("No main program summary data matches the filter.")
    else:
        # Select and rename columns for display
        df_main_program_display = filtered_main_program_df[[
            'program_main_name', 'start_time', 'end_time', 'duration_total', 
            'total_processing_time', 'loss_time'
        ]].rename(columns={
            'program_main_name': 'Main Program',
            'start_time': 'Start Time',
            'end_time': 'End Time',
            'duration_total': 'Duration',
            'total_processing_time': 'Cutting Time',
            'loss_time': 'Loss Time'
        })
        st.dataframe(df_main_program_display, use_container_width=True)
else:
    # This message is shown only if the initial DataFrame is empty
    st.info("No main program summary data is available for this machine within the selected date range.")


st.markdown("---")
st.markdown("#### Sub-Program Cycle")

# Check if the sub-program summary DataFrame is not empty
if not df_subprogram_summary.empty:
    # Use a copy of the DataFrame to avoid modifying the original
    filtered_subprogram_df = df_subprogram_summary.copy()

    # If a main program is selected, filter the DataFrame
    if selected_main_program_input:
        filtered_subprogram_df = filtered_subprogram_df[
            filtered_subprogram_df['program_name'].str.contains(selected_main_program_input, case=False, na=False)
        ]

    # Sort the filtered DataFrame by 'start_time' in descending order
    filtered_subprogram_df = filtered_subprogram_df.sort_values(by='start_time', ascending=False)

    # If the filtered DataFrame is empty, show an info message
    if filtered_subprogram_df.empty:
        st.info("No sub-program summary data found that matches the filter.")
    else:
        # Display the filtered data in a Streamlit table
        st.dataframe(filtered_subprogram_df[[
            'program_name', 'start_time', 'end_time', 'duration_total', 'total_processing_time', 'loss_time'
        ]].rename(columns={
            'program_name': 'Program Name',
            'start_time': 'Start',
            'end_time': 'End',
            'duration_total': 'Duration',
            'total_processing_time': 'Cutting Time',
            'loss_time': 'Loss Time'
        }), use_container_width=True)
else:
    # If the initial DataFrame is empty, show a different info message
    st.info("No sub-program summary data available for this machine within the selected date range.")

st.markdown("---")
st.markdown("#### Sub-Program Cycle Details")
if not df_program_report_display.empty:
    df_display_filtered = df_program_report_display.copy()
    df_display_filtered = df_display_filtered.sort_values(by='start_time', ascending=False)
    df_display_filtered = df_display_filtered.rename(columns={
        "duration": "Cutting Time"
    })

    st.dataframe(df_display_filtered, use_container_width=True)
else:
    st.info(f"Tidak ada data siklus program yang tersedia untuk {selected_machine} dalam rentang tanggal yang dipilih.")

# Otomatis refresh halaman setiap 60 detik (sesuaikan sesuai kebutuhan)
time.sleep(60)
st.rerun()