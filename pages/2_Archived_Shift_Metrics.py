# pages/2_Archived_Shift_Metrics.py

import streamlit as st
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
import logging
import os
import sys

# Konfigurasi logger untuk halaman ini
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Pastikan path ke db_manager dan config sudah benar
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app_core.db_manager import get_shift_metrics_from_db
from app_core.config import SHIFTS, MACHINE_DISPLAY_ORDER

# Konfigurasi halaman
st.set_page_config(layout="wide")

st.title("Archived Shift Metrics") # Judul yang diperbarui untuk kejelasan

# Fungsi untuk mengambil metrik shift final dari database
@st.cache_data(ttl=60)
def fetch_final_shift_metrics(machine_name, start_date, end_date):
    """Mengambil metrik shift final dari database."""
    logger.debug(f"[FinalShiftMetricsPage] Fetching final metrics for machine={machine_name}, start_date={start_date}, end_date={end_date}")
    metrics = get_shift_metrics_from_db(
        machine_name=machine_name,
        shift_name=None, # Mengambil semua shift
        start_date=start_date,
        end_date=end_date,
        is_final=True
    )
    logger.debug(f"[FinalShiftMetricsPage] Fetched {len(metrics)} final metrics.")
    return metrics

# Pilihan mesin
# Menggunakan daftar mesin dari config.py langsung
machine_options = ["Select a Machine"] + list(MACHINE_DISPLAY_ORDER) # Ubah "All Machine" menjadi "All Machines" untuk konsistensi
selected_machine = st.selectbox("Select a Machine", machine_options)

# Kontrol tanggal
start_date = st.date_input("Start Date", datetime.date.today() - datetime.timedelta(days=7)) # Ubah label menjadi "Start Date"
end_date = st.date_input("End Date", datetime.date.today()) # Ubah label menjadi "End Date"

# Fetch dan tampilkan data final shift metrics
if selected_machine != "Select a Machine": # Sesuaikan dengan "All Machines"
    metrics_list = fetch_final_shift_metrics(selected_machine, start_date, end_date)
    
    df_final_shift_metrics = pd.DataFrame(metrics_list)

    if not df_final_shift_metrics.empty:
        st.subheader(f"Metrics Summary for {selected_machine}")
        
        # Mengganti nama kolom untuk tampilan yang lebih baik dan konsistensi
        df_display = df_final_shift_metrics.rename(columns={
            'shift_start_time': 'Start Time', # Ubah 'Start' menjadi 'Start Time'
            'shift_end_time': 'End Time',     # Ubah 'End' menjadi 'End Time'
            'shift_name': 'Shift Name',
            'runtime_seconds': 'Runtime (Seconds)', # Sesuaikan terjemahan
            'idletime_seconds': 'Idletime (Seconds)', # Sesuaikan terjemahan
            'other_time_seconds': 'Other Time (Seconds)', # Sesuaikan terjemahan
            'runtime_hhmm': 'Runtime (HH:MM)',
            'idletime_hhmm': 'Idletime (HH:MM)'
        })

        # Tampilkan tabel data
        st.dataframe(df_display[[
            'Start Time', 'End Time', 'Shift Name', # Sesuaikan nama kolom
            'Runtime (HH:MM)', 'Idletime (HH:MM)', 'Other Time (Seconds)'
        ]])

        # Visualisasi metrik shift (contoh: stacked bar chart)
        df_plot_shift_metrics = df_display.melt(
            id_vars=['Start Time', 'Shift Name'], # Sesuaikan 'Start' menjadi 'Start Time'
            value_vars=['Runtime (Seconds)', 'Idletime (Seconds)', 'Other Time (Seconds)'], # Sesuaikan nama kolom
            var_name='metric',
            value_name='duration_seconds'
        )
        
        # Konversi nama metrik agar lebih mudah dibaca di grafik
        df_plot_shift_metrics['metric'] = df_plot_shift_metrics['metric'].replace({
            'Runtime (Seconds)': 'Runtime',
            'Idletime (Seconds)': 'Idletime',
            'Other Time (Seconds)': 'Other Time'
        })

        fig_shift_metrics = px.bar(
            df_plot_shift_metrics,
            x="Start Time", # Sesuaikan dengan 'Start Time'
            y="duration_seconds",
            color="metric",
            title=f"Time Distribution {selected_machine}",
            labels={"Start Time": "Shift Start Time", "duration_seconds": "Duration (Seconds)", "metric": "Metric"}, # Sesuaikan label
            hover_data={
                "Start Time": "|%Y-%m-%d %H:%M", # Sesuaikan dengan 'Start Time'
                "duration_seconds": ":.2f",
                "metric": True,
                "Shift Name": True # Sesuaikan dengan 'Shift Name'
            },
            color_discrete_map={
                "Runtime": "#28A745", # Ubah "Waktu Berjalan" menjadi "Runtime"
                "Idletime": "#FFC107", # Ubah "Waktu Idle" menjadi "Idletime"
                "Other Time": "#6C757D" # Ubah "Waktu Lain" menjadi "Other Time"
            }
        )
        fig_shift_metrics.update_layout(hovermode="x unified")
        st.plotly_chart(fig_shift_metrics, use_container_width=True)

    else:
        st.info("No archived shift metrics data found for this machine within the selected date range.") # Sesuaikan pesan
elif selected_machine == "All Machines": # Sesuaikan dengan "All Machines"
    st.info("Please select a machine from the dropdown above to view its archived shift metrics.") # Sesuaikan pesan