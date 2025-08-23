# pages/3_Machine_Trend.py

import streamlit as st
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
from app_core.db_manager import (
    get_status_logs_for_machine,
    # get_status_log_table_name, # Tidak lagi diperlukan di sini karena ditangani di dalam fungsi
    get_final_shift_metrics_table_name, # Tetap dibutuhkan untuk nama tabel jika ada fungsi lain yang menggunakannya
    get_shift_metrics_from_db # Mengimpor fungsi umum
)
from app_core.config import SHIFTS # Import SHIFTS dari config.py

# Konfigurasi halaman
st.set_page_config(layout="wide")

st.title("Machine Trend")

# Fungsi untuk mengambil data log status dari database
@st.cache_data(ttl=60) # Cache data selama 60 detik
def fetch_status_logs(machine_name, start_date, end_date):
    """Mengambil log status dari database untuk mesin dan rentang tanggal tertentu."""
    # 16/7/25 --------
    # Panggil get_status_logs_for_machine hanya dengan 3 argumen yang diharapkan
    # Ini akan mengambil log dari SEMUA tabel bulanan yang relevan dalam rentang start_date hingga end_date
    start_dt_obj = datetime.datetime.combine(start_date, datetime.time.min)
    end_dt_obj = datetime.datetime.combine(end_date, datetime.time.max)
    logs = get_status_logs_for_machine(machine_name, start_dt_obj, end_dt_obj)
    return pd.DataFrame(logs)
    # ----------------

# Pilihan mesin
# Anda perlu mendapatkan daftar mesin yang tersedia dari suatu tempat, misalnya dari konfigurasi atau DB
# Untuk contoh ini, kita akan menggunakan daftar dummy
available_machines = ["Makino F5(1) - 1008", "Makino V33 - 1012", "Makino F5(2) - 1009",
                      "Makino V77 - 1000", "Wele 3 - 1006", "Wele 4 - 1007",
                      "Yasda 3 - 1001", "Mitsui Seiki - 1002", "Quaser 4 - 1005",
                      "HSM800 - 1011", "P500 - 1004", "HPM600 - 1010", "HPM800 - 1003"]
selected_machine = st.sidebar.selectbox("Pilih Mesin", available_machines)

# Pemilihan rentang tanggal
date_range = st.sidebar.date_input(
    "Pilih Rentang Tanggal",
    [datetime.date.today() - datetime.timedelta(days=7), datetime.date.today()]
)

start_date = date_range[0]
end_date = date_range[1]

# Ambil data log status
df_status_logs = fetch_status_logs(selected_machine, start_date, end_date)

if not df_status_logs.empty:
    df_status_logs['timestamp'] = pd.to_datetime(df_status_logs['timestamp'], unit='s')
    df_status_logs = df_status_logs.set_index('timestamp').sort_index()

    st.subheader(f"Machine Status Trend for {selected_machine}")

    # Pilihan granularitas
    time_granularity = st.sidebar.selectbox(
        "Granularitas Waktu",
        ["Per Hour", "Per Day", "Per Shift"]
    )

    if time_granularity == "Per Hour":
        rule = "h" # Menggunakan 'h' untuk jam (sesuai saran FutureWarning)
    elif time_granularity == "Per Day":
        rule = "D"
    else: # Per Shift
        # Untuk "Per Shift", kita perlu logika yang lebih kompleks atau menggunakan data metrik shift
        st.warning("Fungsionalitas 'Per Shift' belum sepenuhnya diimplementasikan untuk visualisasi status log. Menampilkan 'Per Hari'.")
        rule = "D"

    # NEW 16/7/25: Revised approach for status aggregation
    df_status_logs['status_count'] = 1 # Helper column for counting

    df_grouped_status = df_status_logs.groupby([pd.Grouper(freq=rule), 'status_text'])['status_count'].sum().reset_index()

    df_pivot_status = df_grouped_status.pivot_table(
        index='timestamp',
        columns='status_text',
        values='status_count'
    ).fillna(0).reset_index()

    df_plot_status = df_pivot_status.melt(
        id_vars=['timestamp'],
        var_name='status',
        value_name='count'
    )

    # --- Pengaturan Warna Kustom untuk Status Mesin ---
    custom_status_colors = {
        'Disconnected': 'black',
        'Connected but not sending data': 'gray',
        'Running': 'green',
        'Manual mode': 'blue',
        'Interrupted': 'orange',
        'Waiting': 'yellow',
        'NC Reset': 'cyan',
        'Emergency': 'red',
        'Ready': 'yellow',
        'With Synchronization': 'magenta',
        'Stop': 'Orange',
        'Hold': 'Orange',
        'N/A': 'gray',
        'MDI': 'purple',
        'Memory': 'blue',
        '****': 'gray',
        'Edit': 'blue',
        'Handle': 'blue',
        'JOG': 'blue',
        'Teach in JOG': 'blue',
        'Teach in Handle': 'blue',
        'INC-feed': 'blue',
        'Reference': 'blue',
        'TEST': 'blue',
        'Faulted': 'red',
    }

    fig_status = px.area(
        df_plot_status,
        x="timestamp",
        y="count",
        color="status",
        title=f"Machine Status Distribution {selected_machine} ({time_granularity})",
        labels={"timestamp": "Time", "count": "Number of Occurrences", "status": "Status"},
        hover_data={"count": True, "status": True},
        color_discrete_map=custom_status_colors
    )
    st.plotly_chart(fig_status, use_container_width=True)

    # Tren Spindle Speed dan Feed Rate
    st.subheader(f"Trends in Spindle Speed and Feedrate for {selected_machine}")
    
    # Filter data yang memiliki nilai spindle_speed atau feed_rate
    df_numeric_trends = df_status_logs.dropna(subset=['spindle_speed', 'feed_rate'], how='all')

    if not df_numeric_trends.empty:
        fig_trends = go.Figure()

        fig_trends.add_trace(go.Scatter(
            x=df_numeric_trends.index,
            y=df_numeric_trends['spindle_speed'],
            mode='lines',
            name='Spindle Speed',
            line=dict(color='blue')
        ))

        fig_trends.add_trace(go.Scatter(
            x=df_numeric_trends.index,
            y=df_numeric_trends['feed_rate'],
            mode='lines',
            name='Feedrate',
            yaxis='y2', # Menggunakan sumbu Y kedua
            line=dict(color='red')
        ))

        # Konfigurasi layout dengan dua sumbu Y
        fig_trends.update_layout(
            title=f"Trends in Spindle Speed and Feedrate for {selected_machine}",
            xaxis_title="Time",
            yaxis=dict(
                title=dict( # <-- PERBAIKAN: Wrap title properties in a 'dict'
                    text="Spindle Speed", # <-- PERBAIKAN: Use 'text' key for title
                    font=dict(color="blue") # <-- PERBAIKAN: 'font' is sub-property of 'title'
                ),
                tickfont=dict(color="blue")
            ),
            yaxis2=dict(
                title=dict( # <-- PERBAIKAN: Wrap title properties in a 'dict'
                    text="Feedrate", # <-- PERBAIKAN: Use 'text' key for title
                    font=dict(color="red") # <-- PERBAIKAN: 'font' is sub-property of 'title'
                ),
                tickfont=dict(color="red"),
                overlaying="y",
                side="right"
            ),
            legend_title="Metrik",
            hovermode="x unified"
        )
        st.plotly_chart(fig_trends, use_container_width=True)
    else:
        st.info("Tidak ada data Spindle Speed atau Feedrate yang tersedia untuk rentang tanggal yang dipilih.")
    
else:
    st.info("Tidak ada data log status yang tersedia untuk rentang tanggal yang dipilih dan mesin ini.")
