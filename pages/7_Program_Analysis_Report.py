# pages/7_Program_Analysis_Report.py

import streamlit as st
import pandas as pd
import datetime
import sys
import os
import logging
import plotly.express as px
from datetime import date, time as dt_time, timezone
from dateutil.relativedelta import relativedelta
from collections import defaultdict

# Menggunakan dt_time untuk menghindari konflik nama
# Import tzlocal untuk deteksi zona waktu lokal, atau fallback ke WIB
try:
    import tzlocal
    local_tz = tzlocal.get_localzone()
except ImportError:
    logging.warning("tzlocal not found. Assuming Asia/Jakarta timezone (WIB) for local time conversion.")
    class AsiaJakartaTZ(datetime.tzinfo):
        def utcoffset(self, dt): return datetime.timedelta(hours=7)
        def dst(self, dt): return datetime.timedelta(0)
        def tzname(self, dt): return "WIB"
    local_tz = AsiaJakartaTZ()

# Pastikan path ke config dan db_manager sudah benar
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app_core.config import MACHINE_DISPLAY_ORDER, IDLE_STATUSES, OTHER_STATUSES
from app_core.db_manager import (
    get_sub_program_analysis_report,
    get_main_program_report,
    init_db_pool,
    db_pool,
    format_seconds_to_hhmm,
    format_seconds_to_hhmmss,
    get_loss_breakdown_report,
    get_loss_breakdown_per_piece_report,
)

def classify_efficiency(efficiency):
    if efficiency >= 85: return "Good"
    elif efficiency >= 75: return "Average"
    else: return "Bad"

def get_status_category_for_loss(status):
    if status in IDLE_STATUSES: return 'IDLE'
    if status in OTHER_STATUSES: return 'OTHER'
    return 'RUNNING_OR_OTHER_KNOWN'

def is_standard_program(program_name):
    if program_name and isinstance(program_name, str) and program_name.strip().upper().startswith('N'):
        return True
    return False

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

# Caching untuk fungsi pengambilan data DB
@st.cache_data(ttl=60) # Cache hasil selama 60 detik
def cached_get_sub_program_analysis_report(machine_name, start_date, end_date, program_name_filter):
    """Memuat laporan efisiensi yang sudah diarsip dari DB dengan caching."""
    return get_sub_program_analysis_report(machine_name, start_date, end_date, program_name_filter)

@st.cache_data(ttl=60)
def cached_get_main_program_report(machine_name, start_date, end_date, program_name_filter):
    """Memuat laporan program induk yang sudah diarsip dari DB dengan caching."""
    return get_main_program_report(machine_name, start_date, end_date, program_name_filter)

# Tambahkan fungsi cached untuk laporan loss breakdown
@st.cache_data(ttl=60)
def cached_get_loss_breakdown_report(machine_name, start_date, end_date):
    return get_loss_breakdown_report(machine_name, start_date, end_date)

@st.cache_data(ttl=60)
def cached_get_loss_breakdown_per_piece_report(machine_name, start_date, end_date):
    return get_loss_breakdown_per_piece_report(machine_name, start_date, end_date)

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Laporan Efisiensi Program")
st.title(f"Program Analysis Report")

# --- Kontrol Filter di Sidebar ---
available_machines = MACHINE_DISPLAY_ORDER
if not available_machines:
    st.warning("Tidak ada mesin yang terdaftar dalam konfigurasi.")
    st.stop()

selected_machine = st.sidebar.selectbox("Select a Machine", options=["All"] + available_machines)
if selected_machine == "All":
    selected_machine = None

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=datetime.date.today() - datetime.timedelta(days=30))
with col2:
    end_date = st.date_input("End Date", value=datetime.date.today())

# Fetch main program data to populate the list
# We call this outside the conditional to ensure we have the list even when the filter is empty
all_main_program_names = []
machines_for_list = available_machines if selected_machine is None else [selected_machine]
for machine in machines_for_list:
    main_program_reports = cached_get_main_program_report(machine, start_date, end_date, None)
    if main_program_reports:
        df_main_programs = pd.DataFrame(main_program_reports)
        # Create a list of formatted strings: "Program Name - Machine (Start Time)"
        formatted_list = []
        for index, row in df_main_programs.iterrows():
            local_start_time = pd.to_datetime(row['session_start_time']).tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')
            formatted_list.append(f"{row['program_main_name']} - {row['machine_name']} ({local_start_time})")
        all_main_program_names.extend(formatted_list)
# Remove duplicates and sort the list
unique_main_program_names = sorted(list(set(all_main_program_names)))

program_name_filter = st.sidebar.text_input("Main Program", "")
program_name_filter = program_name_filter.strip() if program_name_filter else None

if start_date > end_date:
    st.sidebar.error("Tanggal mulai tidak boleh lebih lambat dari tanggal akhir.")
    st.stop()
    
# Jika memilih "All" machines, kita harus membuat loop untuk setiap mesin yang tersedia
if selected_machine is None:
    machines_to_process = available_machines
else:
    machines_to_process = [selected_machine]

all_report_data_raw = []
for machine in machines_to_process:
    report_data_raw_for_machine = cached_get_sub_program_analysis_report(machine, start_date, end_date, program_name_filter)
    all_report_data_raw.extend(report_data_raw_for_machine)

if not all_report_data_raw:
    st.info("Tidak ada laporan yang tersimpan dalam rentang tanggal dan filter yang dipilih.")
    Total_overall_duration_seconds = 0
    Total_overall_duration_per_piece_seconds = 0
    Total_cycle_time_seconds = 0
    Total_loss_time_seconds = 0
    Total_cycle_time_per_pieces_seconds = 0
    Total_loss_time_per_piece_seconds = 0
    df_loss_breakdown = pd.DataFrame()
    df_loss_breakdown_per_piece = pd.DataFrame()

    if program_name_filter is None and unique_main_program_names:
            st.write("Daftar Program Induk yang Tersedia:")
            st.code('\n'.join(unique_main_program_names))
            
    st.stop()

df_report = pd.DataFrame(all_report_data_raw)
df_report['report_date'] = pd.to_datetime(df_report['report_date']).dt.date
df_report['archived_at'] = pd.to_datetime(df_report['archived_at'])

if 'id' in df_report.columns:
    df_report = df_report.drop(columns=['id'])

df_display = df_report.copy()
df_display['actual_avg_duration_hhmm'] = df_display['actual_avg_duration_seconds'].apply(format_seconds_to_hhmmss)
df_display['target_duration_hhmm'] = df_display['target_duration_seconds'].apply(format_seconds_to_hhmmss)
df_display['efficiency_status'] = df_display['efficiency_percent'].apply(classify_efficiency)

Total_target_duration_seconds = df_display['target_duration_seconds'].sum()
Total_target_duration_hhmmss = format_seconds_to_hhmmss(Total_target_duration_seconds)

Total_actual_avg_duration_seconds = df_display['actual_avg_duration_seconds'].sum()
Total_actual_avg_duration_hhmmss = format_seconds_to_hhmmss(Total_actual_avg_duration_seconds)

avg_efficiency_percent = df_display['efficiency_percent'].mean()

df_display = df_display[[
    'machine_name',
    'program_name',
    'target_duration_hhmm',
    'actual_avg_duration_hhmm',
    'efficiency_percent',
    'efficiency_status',
    'target_spindle_speed',
    'actual_spindle_speed_mode',
    'target_feed_rate',
    'actual_feed_rate_mode',
    'notes'
]].rename(columns={
    'machine_name': 'Machine',
    'program_name': 'Program',
    'target_duration_hhmm': 'Target',
    'actual_avg_duration_hhmm': 'Actual',
    'efficiency_percent': 'Eff (%)',
    'efficiency_status': 'State',
    'target_spindle_speed': 'Prog. RPM',
    'actual_spindle_speed_mode': 'Act. RPM',
    'target_feed_rate': 'Prog. Feed (mm/min)',
    'actual_feed_rate_mode': 'Act. Feed (mm/min)',
    'notes': 'Note'
})

df_summary_of_subprogram_analysis = pd.DataFrame([{
    'Total Target Duration (HH:MM:SS)': Total_target_duration_hhmmss,
    'Total Actual Duration (HH:MM:SS)': Total_actual_avg_duration_hhmmss,
    'Efficiency (%)': f"{avg_efficiency_percent:.2f}"
}])

df_display = df_display.sort_values(by=['Machine', 'Program'], ascending=[True, True])

if program_name_filter == None:
    st.info("Enter the main program name")
    st.write("List of available main programs:")
    if unique_main_program_names:
        st.code('\n'.join(unique_main_program_names))
    else:
        st.info("Tidak ada data program induk yang tersimpan dalam rentang tanggal yang dipilih.")
    st.stop()

st.subheader(f"Data per Sub-Program")
st.dataframe(df_display, use_container_width=True, hide_index=True)

st.markdown(f"#### Summary of Sub-Program Analysis")
st.dataframe(df_summary_of_subprogram_analysis, use_container_width=True, hide_index=True)


st.subheader("Visualization of Archived Program Efficiency")
df_chart = df_display.copy()
# --- PERBAIKAN: Mengurutkan grafik dari efisiensi tertinggi ke terendah ---
df_chart = df_chart.sort_values(by='Eff (%)', ascending=False)
fig_efficiency = px.bar(
    df_chart,
    x='Program',
    y='Eff (%)',
    color='State',
    color_discrete_map={
        "Good": "green",
        "Average": "orange",
        "Bad": "red"
    },
    title=f"Archived Program Efficiency",
    hover_data={
        'Actual': True,
        'Target': True,
        'Eff (%)': ':.2f',
        'Act. RPM': ':.0f',
        'Act. Feed (mm/min)': ':.0f',
        'Prog. RPM': ':.0f',
        'Prog. Feed (mm/min)': ':.0f',
        'Note': True
    }
)
fig_efficiency.update_layout(
    xaxis_title="Program Name",
    yaxis_title="Efficiency (%)",
    yaxis_range=[0, 100]
)
st.plotly_chart(fig_efficiency, use_container_width=True)


# --- Re-integrasi Analisis Level Program Induk & Visualisasi ---
st.markdown("---")
st.header("Main Program Analysis")

df_program_induk_raw = cached_get_main_program_report(selected_machine, start_date, end_date, program_name_filter)

# PERBAIKAN: Cek apakah ada data Program Induk sebelum memproses
if not df_program_induk_raw:
    st.info("Tidak ada data Program Induk yang ditemukan dalam laporan arsip.")
    # Inisialisasi DataFrame kosong agar tidak terjadi error pada bagian chart
    Total_overall_duration_seconds = 0
    Total_overall_duration_per_piece_seconds = 0
    Total_cycle_time_seconds = 0
    Total_loss_time_seconds = 0
    Total_cycle_time_per_pieces_seconds = 0
    Total_loss_time_per_piece_seconds = 0
else:
    df_program_induk = pd.DataFrame(df_program_induk_raw)
    Total_overall_duration_seconds = df_program_induk['total_process_time_seconds'].sum()
    Total_overall_duration_hhmmss = format_seconds_to_hhmmss(Total_overall_duration_seconds)
    Total_loss_time_seconds = df_program_induk['total_loss_time_seconds'].sum()
    Total_loss_time_hhmmss = format_seconds_to_hhmmss(Total_loss_time_seconds)
    Total_cycle_time_seconds = df_program_induk['cycle_time_seconds'].sum()
    Total_cycle_time_hhmmss = format_seconds_to_hhmmss(Total_cycle_time_seconds)

    #Hitung per pieces
    df_program_induk['total_process_time_per_pieces_seconds'] = df_program_induk.apply(
        lambda row: row['total_process_time_seconds'] / row['quantity'] if row['quantity'] > 0 else 0, axis=1)
    df_program_induk['total_process_time_per_pieces_hhmmss'] = df_program_induk['total_process_time_per_pieces_seconds'].apply(format_seconds_to_hhmmss)
    
    df_program_induk['total_loss_time_per_pieces_seconds'] = df_program_induk.apply(
        lambda row: row['total_loss_time_seconds'] / row['quantity'] if row['quantity'] > 0 else 0, axis=1)
    df_program_induk['total_loss_time_per_pieces_hhmmss'] = df_program_induk['total_loss_time_per_pieces_seconds'].apply(format_seconds_to_hhmmss)

    df_program_induk['cycle_time_per_pieces_seconds'] = df_program_induk.apply(
        lambda row: row['cycle_time_seconds'] / row['quantity'] if row['quantity'] > 0 else 0, axis=1)
    df_program_induk['cycle_time_per_pieces_hhmmss'] = df_program_induk['cycle_time_per_pieces_seconds'].apply(format_seconds_to_hhmmss)

    Total_overall_duration_per_piece_seconds = df_program_induk['total_process_time_per_pieces_seconds'].sum()
    Total_overall_duration_per_piece_hhmmss = format_seconds_to_hhmmss(Total_overall_duration_per_piece_seconds)

    Total_loss_time_per_piece_seconds = df_program_induk["total_loss_time_per_pieces_seconds"].sum()
    Total_loss_time_per_piece_hhmmss = format_seconds_to_hhmmss(Total_loss_time_per_piece_seconds)
    
    Total_cycle_time_per_pieces_seconds = df_program_induk["cycle_time_per_pieces_seconds"].sum()
    Total_cycle_time_per_pieces_hhmmss = format_seconds_to_hhmmss(Total_cycle_time_per_pieces_seconds)

    # Tampilkan data utama
    st.markdown("#### Main Program Cycle Time")

    df_program_induk_display = df_program_induk[[
        'machine_name',
        'program_main_name',
        'session_start_time',
        'session_end_time',
        'total_process_time_seconds',
        'total_loss_time_seconds',
        'cycle_time_seconds',
        'quantity',
        'total_process_time_per_pieces_hhmmss',
        'total_loss_time_per_pieces_hhmmss',
        'cycle_time_per_pieces_hhmmss'
    ]].rename(columns={
        'machine_name': 'Machine',
        'program_main_name': 'Main Program',
        'session_start_time': 'Start Time',
        'session_end_time': 'End Time',
        'total_process_time_seconds': 'Duration (sec)',
        'total_loss_time_seconds': 'Loss Time (sec)',
        'cycle_time_seconds': 'Cutting Time (sec)',
        'quantity': 'Qty',
        'total_process_time_per_pieces_hhmmss': 'Duration/pcs',
        'total_loss_time_per_pieces_hhmmss': 'Loss Time/pcs',
        'cycle_time_per_pieces_hhmmss': 'Cutting Time/pcs'
    })

    # Menambahkan kolom durasi yang diformat
    df_program_induk_display['Duration'] = df_program_induk_display['Duration (sec)'].apply(format_seconds_to_hhmmss)
    df_program_induk_display['Loss Time'] = df_program_induk_display['Loss Time (sec)'].apply(format_seconds_to_hhmmss)
    df_program_induk_display['Cutting Time'] = df_program_induk_display['Cutting Time (sec)'].apply(format_seconds_to_hhmmss)
    df_program_induk_display['Start Time'] = pd.to_datetime(df_program_induk_display['Start Time']).dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')
    df_program_induk_display['End Time'] = pd.to_datetime(df_program_induk_display['End Time']).dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')

    # PERBAIKAN: Mengurutkan tabel berdasarkan Start Time secara ascending
    df_program_induk_display = df_program_induk_display.sort_values(by='Start Time', ascending=True)

    # Urutkan dan tampilkan
    st.dataframe(df_program_induk_display[[
        'Machine', 'Main Program', 'Start Time', 'End Time',
        'Duration', 'Loss Time', 'Cutting Time', 'Qty', 'Duration/pcs', 'Loss Time/pcs', 'Cutting Time/pcs'
    ]], use_container_width=True, hide_index=True)
    st.write("Notes")

    # Gabungkan data dari kolom 'notes' dan 'notes_qty' menjadi satu list
    combined_notes = df_program_induk['notes'].dropna().tolist() + df_program_induk['notes_qty'].dropna().tolist()

    # (Opsional) Urutkan list yang sudah digabungkan secara descending
    combined_notes.sort(reverse=True)

    # Tampilkan semua data dalam satu perulangan
    for item in combined_notes:
        if item:  # Pastikan hanya menampilkan item yang tidak kosong
            st.write(item)
    
    
    st.markdown("##### Total Durations (All Sessions)")
    df_total_induk = pd.DataFrame([{
        'Total Duration': Total_overall_duration_hhmmss,
        'Total Loss Time': Total_loss_time_hhmmss,
        'Total Cutting Time': Total_cycle_time_hhmmss,
        'Total Duration/pcs': Total_overall_duration_per_piece_hhmmss,
        'Total Loss Time/pcs':Total_loss_time_per_piece_hhmmss,
        'Total Cutting Time/pcs': Total_cycle_time_per_pieces_hhmmss
    }])
    st.dataframe(df_total_induk, use_container_width=True, hide_index=True)

    # --- BARU: Ambil data loss breakdown dari database ---
    df_loss_breakdown_raw = cached_get_loss_breakdown_report(selected_machine, start_date, end_date)
    df_loss_breakdown = pd.DataFrame(df_loss_breakdown_raw)
    
    df_loss_breakdown_per_piece_raw = cached_get_loss_breakdown_per_piece_report(selected_machine, start_date, end_date)
    df_loss_breakdown_per_piece = pd.DataFrame(df_loss_breakdown_per_piece_raw)
    # --- AKHIR PERBAIKAN ---

# --- Visualization: Pie Charts Berdampingan ---
st.markdown("---")
st.header("Visualisasi Efisiensi dan Waktu Loss")

# Buat empat kolom untuk menempatkan empat pie chart
col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

# Chart 1: Total Duration Breakdown
with col1:
    st.subheader("Total Duration Breakdown")
    if Total_overall_duration_seconds > 0:
        data_for_pie = {
            'Category': ['Cutting Time', 'Loss Time'],
            'Duration (seconds)': [Total_cycle_time_seconds, Total_loss_time_seconds]
        }
        df_pie = pd.DataFrame(data_for_pie)
        
        fig_pie = px.pie(
            df_pie,
            values='Duration (seconds)',
            names='Category',
            title=f"Total Duration for {selected_machine}",
            color_discrete_map={'Cutting Time': '#0096C7', 'Loss Time': '#FFB703'},
            hole=0.4
        )
        fig_pie.update_traces(textinfo='percent+label')
        fig_pie.update_layout(showlegend=False)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Tidak ada data durasi untuk chart ini.")

# Chart 2: Total Duration Breakdown per Pieces
with col2:
    st.subheader("Duration/Pcs Breakdown")
    if Total_overall_duration_per_piece_seconds > 0:
        data_for_pie_per_pieces = {
            'Category': ['Cutting Time/Pcs', 'Loss Time/Pcs'],
            'Duration (seconds)': [Total_cycle_time_per_pieces_seconds, Total_loss_time_per_piece_seconds]
        }
        df_pie_per_pieces = pd.DataFrame(data_for_pie_per_pieces)
        
        fig_pie_per_pieces = px.pie(
            df_pie_per_pieces,
            values='Duration (seconds)',
            names='Category',
            title=f"Duration/Pcs for {selected_machine}",
            color_discrete_map={'Cutting Time/Pcs': '#0096C7', 'Loss Time/Pcs': '#FFB703'},
            hole=0.4
        )
        fig_pie_per_pieces.update_traces(textinfo='percent+label')
        fig_pie_per_pieces.update_layout(showlegend=False)
        st.plotly_chart(fig_pie_per_pieces, use_container_width=True)
    else:
        st.info("Tidak ada data durasi per pieces.")

# Chart 3: Loss Time Breakdown
with col3:
    st.subheader("Loss Time Breakdown")
    # PERBAIKAN: Periksa apakah DataFrame kosong sebelum membuat chart
    if not df_loss_breakdown.empty:
        fig_loss_pie = px.pie(
            df_loss_breakdown,
            values='duration_seconds',
            names='loss_category',
            title=f"Loss Time Breakdown",
            hole=0.4
        )
        fig_loss_pie.update_traces(textinfo='percent+label')
        fig_loss_pie.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        st.plotly_chart(fig_loss_pie, use_container_width=True)
    else:
        st.info("Tidak ada data loss time.")

# Chart 4: Loss Time Breakdown per Pieces
with col4:
    st.subheader("Loss Time/Pcs Breakdown")
    # PERBAIKAN: Periksa apakah DataFrame kosong sebelum membuat chart
    if not df_loss_breakdown_per_piece.empty:
        fig_loss_pie_per_piece = px.pie(
            df_loss_breakdown_per_piece,
            values='duration_seconds',
            names='loss_category',
            title=f"Loss Time/Pcs Breakdown",
            hole=0.4
        )
        fig_loss_pie_per_piece.update_traces(textinfo='percent+label')
        fig_loss_pie_per_piece.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        st.plotly_chart(fig_loss_pie_per_piece, use_container_width=True)
    else:
        st.info("Tidak ada data loss time per pieces.")