# pages/6_Program_Efficiency_Analysis.py

import streamlit as st
import pandas as pd
import datetime
import sys
import os
import plotly.graph_objects as go
import plotly.express as px
import logging
import time
from datetime import date, time as dt_time, timezone
from dateutil.relativedelta import relativedelta
from app_core.data_processor import get_mode 
from app_core.csv_converter import process_raw_csv_data, convert_time_to_seconds


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
from app_core.config import (
    MACHINE_DISPLAY_ORDER,
    IDLE_STATUSES, # Import IDLE_STATUSES dan OTHER_STATUSES
    OTHER_STATUSES
)
from app_core.db_manager import (
    format_seconds_to_hhmm,
    format_seconds_to_hhmmss,
    get_program_report_from_db,
    get_program_report_from_db2, 
    init_db_pool,
    db_pool,
    close_db_connection,
    get_status_logs_for_machine,
    save_sub_program_analysis_report, 
    update_program_name_in_db,
    save_main_program_analysis,
    save_loss_breakdown_report,
    save_loss_breakdown_per_piece_report 
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

# --- Caching untuk fungsi pengambilan data DB ---
@st.cache_data(ttl=60) # Cache hasil selama 60 detik
def cached_get_program_report_from_db(machine_name, start_date, end_date):
    """Memuat laporan program dari DB dengan caching."""
    return get_program_report_from_db(machine_name, start_date, end_date)

@st.cache_data(ttl=120) # Cache untuk status log
def cached_get_status_logs_for_machine(machine_name, start_time, end_time):
    """Memuat status log dari DB dengan caching."""
    return get_status_logs_for_machine(machine_name, start_time, end_time)

@st.cache_data(ttl=120)
def cached_get_program_report_from_db2(machine_name, start_date, end_date, specific_program_filter):
    """Memuat log status detail untuk program induk dengan caching."""
    return get_program_report_from_db2(machine_name, start_date, end_date, specific_program_filter)

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Analisa Efisiensi Program")
st.title("Program Analysis")

# --- Kontrol Filter di Sidebar ---
available_machines = MACHINE_DISPLAY_ORDER
if not available_machines:
    st.warning("Tidak ada mesin yang terdaftar dalam konfigurasi.")
    st.stop()

selected_machine = st.sidebar.selectbox("Select a Machine", options=available_machines)

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=datetime.date.today() - datetime.timedelta(days=7))
with col2:
    end_date = st.date_input("End Date", value=datetime.date.today())

if start_date > end_date:
    st.sidebar.error("Tanggal mulai tidak boleh lebih lambat dari tanggal akhir.")
    st.stop()

# Load data program report (durasi siklus program)
program_logs = cached_get_program_report_from_db(selected_machine, start_date, end_date)

# Load raw status logs (untuk spindle/feedrate)
start_datetime_utc = datetime.datetime.combine(start_date, dt_time.min, tzinfo=datetime.timezone.utc)
end_datetime_utc = datetime.datetime.combine(end_date, dt_time.max, tzinfo=datetime.timezone.utc)

raw_status_logs = cached_get_status_logs_for_machine(selected_machine, start_datetime_utc, end_datetime_utc)

if not program_logs:
    st.info(f"Tidak ada data program yang tersedia untuk {selected_machine} dalam rentang tanggal yang dipilih ({start_date} hingga {end_date}).")
    st.stop()

df_program = pd.DataFrame(program_logs)

# --- Gabungkan data program dengan data status log untuk spindle/feedrate (menggunakan modus) ---
if raw_status_logs:
    df_raw_status = pd.DataFrame(raw_status_logs)
    df_raw_status['timestamp_log'] = pd.to_datetime(df_raw_status['timestamp'], utc=True)
    
    df_raw_status['program_main_name'] = df_raw_status['current_program'].apply(
        lambda x: str(x).split('-')[0].strip() if x and '-' in str(x) else None
    )
    
    df_running_status = df_raw_status[df_raw_status['status_text'].isin(['Running'])].copy()

    if not df_running_status.empty:
        df_running_status['program_name'] = df_running_status['current_program'].fillna('N/A')

        df_program_spindle_feed = df_running_status.groupby('program_name').agg(
            most_common_spindle_speed=('spindle_speed', get_mode),
            most_common_feed_rate=('feed_rate', get_mode)
        ).reset_index()

        df_program = pd.merge(df_program, df_program_spindle_feed, on='program_name', how='left')
        df_program[['most_common_spindle_speed', 'most_common_feed_rate']] = \
            df_program[['most_common_spindle_speed', 'most_common_feed_rate']].fillna(0).round(0)
    else:
        df_program['most_common_spindle_speed'] = 0.0
        df_program['most_common_feed_rate'] = 0.0
else:
    df_program['most_common_spindle_speed'] = 0.0
    df_program['most_common_feed_rate'] = 0.0


# --- Ekstraksi namaInduk dan filter dengan input manual ---
df_program['program_main_name'] = df_program['program_name'].apply(
    lambda x: str(x).split('-')[0].strip() if x and '-' in str(x) else str(x).strip()
)

selected_main_program_input = st.sidebar.text_input(
    "Main Program",
    value=st.session_state.get('main_program_filter_input', ""),
    key='main_program_filter_input'
).strip()

if selected_main_program_input:
    df_program = df_program[
        df_program['program_main_name'].str.contains(selected_main_program_input, case=False, na=False)
    ]
    if df_program.empty:
        st.info(f"Tidak ada program yang mengandung '{selected_main_program_input}' pada {selected_machine} dalam rentang tanggal yang dipilih.")
        st.stop()

# Ringkas data untuk mendapatkan daftar program unik dan durasi rata-rata aktual
df_program_summary_actual = df_program.groupby('program_name').agg(
    total_cycle_duration_seconds=('duration_seconds', 'sum'),
    most_common_spindle_speed=('most_common_spindle_speed', lambda x: x.mode().iloc[0] if not x.mode().empty else 0),
    most_common_feed_rate=('most_common_feed_rate', lambda x: x.mode().iloc[0] if not x.mode().empty else 0)
).reset_index()

uploaded_file = st.file_uploader("Import Data from CSV", type=['csv'], key="target_csv_uploader")

if uploaded_file is not None:
    try:
        st.info("Memproses file CSV yang diunggah...")
        file_name = uploaded_file.name.split('.')[0]
        try:
            df_imported_targets_raw = pd.read_csv(uploaded_file, header=2, encoding='latin-1')
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            df_imported_targets_raw = pd.read_csv(uploaded_file, header=2, encoding='cp1252')
        
        df_imported_targets = process_raw_csv_data(df_imported_targets_raw, file_name)

        # Update session_state dengan data yang sudah diproses
        st.info("Memperbarui target dari CSV...")
        for _, row_csv in df_imported_targets.iterrows():
            program_name_from_csv = str(row_csv['program_name']).strip()
            if program_name_from_csv in df_program_summary_actual['program_name'].values:
                st.session_state[f"target_minutes_{program_name_from_csv}"] = row_csv['target_duration (min)']
                st.session_state[f"target_spindle_{program_name_from_csv}"] = row_csv['Spindle RPM']
                st.session_state[f"target_feedrate_{program_name_from_csv}"] = row_csv['target_feedrate']
                st.session_state[f"quantity_{program_name_from_csv}"] = row_csv['Quantity']
                st.session_state[f"notes_{program_name_from_csv}"] = row_csv['Notes']
                st.session_state[f"remarks_{program_name_from_csv}"] = row_csv['Remarks']
                
        st.session_state.rebuild_editor_data_sub = True
        st.success("Target berhasil diimpor dari CSV! Tabel akan diperbarui.")
        st.rerun()

    except Exception as e:
        st.error(f"Terjadi kesalahan saat membaca atau memproses file CSV: {e}")
        logging.error(f"Error reading CSV: {e}", exc_info=True)


# Inisialisasi/perbarui session_state untuk setiap program (sub-program)
if 'editable_program_data' not in st.session_state or \
   st.session_state.editable_program_data_machine != selected_machine or \
   st.session_state.editable_program_data_start_date != start_date or \
   st.session_state.editable_program_data_end_date != end_date or \
   st.session_state.editable_program_data_main_program_filter != selected_main_program_input or \
   st.session_state.get('rebuild_editor_data_sub', False):
    
    st.session_state.editable_program_data = []
    st.session_state.editable_program_data_machine = selected_machine
    st.session_state.editable_program_data_start_date = start_date
    st.session_state.editable_program_data_end_date = end_date
    st.session_state.editable_program_data_main_program_filter = selected_main_program_input

    for _, row in df_program_summary_actual.iterrows():
        program_name = row['program_name']
        
        target_minutes = st.session_state.get(f"target_minutes_{program_name}", 0.0)
        target_spindle = st.session_state.get(f"target_spindle_{program_name}", 0)
        target_feedrate = st.session_state.get(f"target_feedrate_{program_name}", 0)
        quantity = st.session_state.get(f"quantity_{program_name}", 1)
        notes = st.session_state.get(f"notes_{program_name}", "")
        remarks = st.session_state.get(f"remarks_{program_name}", "")

        st.session_state.editable_program_data.append({
            'Program Name': program_name,
            'Duration (min)': float(target_minutes),
            'Spindle Speed (RPM)': int(target_spindle),
            'Feedrate (mm/min)': int(target_feedrate),
            'Qty': int(quantity),
            'Note': str(notes),
        })
    
    df_editable_input_sub = pd.DataFrame(st.session_state.editable_program_data)

    st.session_state.rebuild_editor_data_sub = False
else:
    df_editable_input_sub = pd.DataFrame(st.session_state.editable_program_data)


edited_df_with_inputs_sub = st.data_editor(
    df_editable_input_sub,
    key="program_efficiency_editor_sub",
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "Program Name": st.column_config.TextColumn("Program Name", help="Program Name", disabled=False),
        "Duration (min)": st.column_config.NumberColumn("Duration (min)", help="Target durasi program dalam menit desimal", min_value=0.0, format="%.1f"),
        "Spindle Speed (RPM)": st.column_config.NumberColumn("Spindle Speed (RPM)", help="Target Spindle Speed (RPM)", min_value=0, format="%d"),
        "Feedrate (mm/min)": st.column_config.NumberColumn("Feedrate (mm/min)", help="Target Feed Rate (mm/min)", min_value=0, format="%d"),
        "Qty": st.column_config.NumberColumn("Qty", help="Jumlah unit yang dihasilkan dalam durasi aktual", min_value=1, format="%d"),
        "Note": st.column_config.TextColumn("Note", help="Catatan tambahan untuk program ini", width="large")
    }
)

# Tangkap editan dan perbarui session_state
if st.session_state.program_efficiency_editor_sub:
    for program_idx, edited_cols in st.session_state.program_efficiency_editor_sub['edited_rows'].items():
        original_program_name = df_editable_input_sub.loc[program_idx, 'Program Name']

        # Check if the program name was edited
        if 'Program Name' in edited_cols:
            new_program_name = edited_cols['Program Name']
            if new_program_name != original_program_name:
                st.session_state[f"pending_rename_sub_{original_program_name}"] = new_program_name
                st.warning(f"Program '{original_program_name}' has been changed to '{new_program_name}'. Click 'Save Sub-Program Changes to Database' to apply.")

        # Handle other column edits
        if 'Duration (min)' in edited_cols:
            st.session_state[f"target_minutes_{original_program_name}"] = edited_cols['Duration (min)']
        if 'Spindle Speed (RPM)' in edited_cols:
            st.session_state[f"target_spindle_{original_program_name}"] = edited_cols['Spindle Speed (RPM)']
        if 'Feedrate (mm/min)' in edited_cols:
            st.session_state[f"target_feedrate_{original_program_name}"] = edited_cols['Feedrate (mm/min)']
        if 'Qty' in edited_cols:
            st.session_state[f"quantity_{original_program_name}"] = edited_cols['Qty']
        if 'Note' in edited_cols:
            st.session_state[f"notes_{original_program_name}"] = edited_cols['Note']
            
# Perbarui DataFrame yang akan digunakan untuk perhitungan
df_editable_input_sub = edited_df_with_inputs_sub.rename(columns={
    'Program Name': 'program_name',
    'Duration (min)': 'Target Durasi (menit)',
    'Spindle Speed (RPM)': 'Target RPM',
    'Feedrate (mm/min)': 'Target Feed Rate (mm/min)',
    'Qty': 'Quantity',
    'Note': 'Catatan'
})

# Add a button to save all changes
if st.button("Save Sub-Program Changes to Database"):
    success = True
    for key, new_name in list(st.session_state.items()): # Gunakan list() untuk menghindari error saat iterasi dan menghapus
        if key.startswith("pending_rename_sub_"):
            old_name = key.replace("pending_rename_sub_", "")
            if not update_program_name_in_db(old_name, new_name, selected_machine, start_date, end_date):
                success = False
                st.error(f"Gagal memperbarui nama program untuk '{old_name}'.")
            del st.session_state[key]
    
    if success:
        st.success("Semua perubahan berhasil disimpan ke database! Memperbarui data...")
        st.cache_data.clear()
        st.rerun()
    else:
        st.error("Beberapa perubahan gagal disimpan. Periksa log untuk detail.")

# Perbarui df_program_summary_actual dengan nilai yang mungkin sudah diedit dari st.data_editor
df_program_summary_actual['target_duration_seconds'] = df_program_summary_actual['program_name'].apply(
    lambda p_name: float(st.session_state.get(f"target_minutes_{p_name}", 0.0)) * 60
)
df_program_summary_actual['target_duration_hhmmss'] = df_program_summary_actual['target_duration_seconds'].apply(format_seconds_to_hhmmss)

df_program_summary_actual['target_spindle_speed'] = df_program_summary_actual['program_name'].apply(
    lambda p_name: int(st.session_state.get(f"target_spindle_{p_name}", 0))
)
df_program_summary_actual['target_feed_rate'] = df_program_summary_actual['program_name'].apply(
    lambda p_name: int(st.session_state.get(f"target_feedrate_{p_name}", 0))
)
df_program_summary_actual['quantity'] = df_program_summary_actual['program_name'].apply(
    lambda p_name: int(st.session_state.get(f"quantity_{p_name}", 1))
)
df_program_summary_actual['notes'] = df_program_summary_actual['program_name'].apply(
    lambda p_name: str(st.session_state.get(f"notes_{p_name}", ""))
)

df_program_summary_actual['actual_avg_duration_per_piece_seconds'] = df_program_summary_actual.apply(
    lambda row: row['total_cycle_duration_seconds'] / row['quantity'] if row['quantity'] > 0 else 0.0,
    axis=1
)
df_program_summary_actual['actual_avg_duration_per_piece_hhmmss'] = df_program_summary_actual['actual_avg_duration_per_piece_seconds'].apply(format_seconds_to_hhmmss)

Total_target_duration_seconds = df_program_summary_actual['target_duration_seconds'].sum()
Total_target_duration_hhmmss = format_seconds_to_hhmmss(Total_target_duration_seconds)

Total_actual_avg_duration_per_piece_seconds = df_program_summary_actual['actual_avg_duration_per_piece_seconds'].sum()
Total_actual_avg_duration_per_piece_hhmmss = format_seconds_to_hhmmss(Total_actual_avg_duration_per_piece_seconds)


df_efficiency = df_program_summary_actual[df_program_summary_actual['target_duration_seconds'] > 0].copy()

if df_efficiency.empty:
    st.info("Tidak ada program dengan target durasi yang valid untuk dianalisa efisiensinya dalam filter yang dipilih.")
    st.stop()

df_efficiency['efficiency_percent'] = df_efficiency.apply(
    lambda row: min(100.0, (row['target_duration_seconds'] / row['actual_avg_duration_per_piece_seconds']) * 100)
    if row['actual_avg_duration_per_piece_seconds'] > 0 else 0.0,
    axis=1
)

df_efficiency['efficiency_percent'] = df_efficiency['efficiency_percent'].round(2)

def classify_efficiency(efficiency):
    if efficiency >= 85: return "Good"
    elif efficiency >= 75: return "Average"
    else: return "Bad"

df_efficiency['efficiency_status'] = df_efficiency['efficiency_percent'].apply(classify_efficiency)

st.markdown("#### Summary of Sub-program Analysis")
remarks_value = st.session_state.get(f"remarks_{program_name}", "Nilai Remarks belum tersedia.")
st.write(f"{remarks_value}")

# Buat DataFrame untuk tampilan dengan kolom yang diinginkan
df_efficiency_display = df_efficiency[[
    'program_name',
    'target_duration_hhmmss',
    'actual_avg_duration_per_piece_hhmmss',
    'quantity',
    'target_spindle_speed',
    'most_common_spindle_speed',
    'target_feed_rate',
    'most_common_feed_rate',
    'efficiency_percent',
    'efficiency_status',
    'notes'
]].rename(columns={
    'program_name': 'Program',
    'target_duration_hhmmss': 'Target Duration/pcs',
    'actual_avg_duration_per_piece_hhmmss': 'Actual Duration/pcs',
    'quantity': 'Qty',
    'target_spindle_speed': 'Prog. RPM',
    'most_common_spindle_speed': 'Act. RPM',
    'target_feed_rate': 'Prog. Feed (mm/min)',
    'most_common_feed_rate': 'Act. Feed (mm/min)',
    'efficiency_percent': 'Eff (%)',
    'efficiency_status': 'State',
    'notes': 'Note'
})

st.dataframe(df_efficiency_display, use_container_width=True)

# Tampilkan baris total secara terpisah
st.markdown("##### Total Durations")
df_total_sub = pd.DataFrame([{
    'Total Target Duration (HH:MM:SS)': Total_target_duration_hhmmss,
    'Total Actual Avg Duration (HH:MM:SS)': Total_actual_avg_duration_per_piece_hhmmss
}])
st.dataframe(df_total_sub, use_container_width=True, hide_index=True)

st.markdown("#### Visualization of Sub-program Efficiency")

df_efficiency_sorted = df_efficiency.sort_values(by='efficiency_percent', ascending=False)

fig_efficiency = px.bar(
    df_efficiency_sorted,
    x='program_name',
    y='efficiency_percent',
    color='efficiency_status',
    color_discrete_map={
        "Good": "green",
        "Average": "orange",
        "Bad": "red"
    },
    title=f"Sub-program Efficiency {selected_machine}",
    hover_data={
        'actual_avg_duration_per_piece_hhmmss': True,
        'target_duration_hhmmss': True,
        'efficiency_percent': ':.2f',
        'most_common_spindle_speed': ':.0f',
        'most_common_feed_rate': ':.0f',
        'target_spindle_speed': ':.0f',
        'target_feed_rate': ':.0f',
        'quantity': ':.0f',
        'notes': True
    }
)
fig_efficiency.update_layout(
    xaxis_title="Program Name",
    yaxis_title="Efficency (%)",
    yaxis_range=[0, 100]
)
st.plotly_chart(fig_efficiency, use_container_width=True)


# --- Analisis Level Program Induk ---
st.markdown("---")
st.header("Main Program Analysis")

# 1. Agregasi data ke level Program Induk
temp_df_induk_summary = df_program.groupby('program_main_name').agg(
    program_induk_overall_start_time=('start_time', 'min'),
    program_induk_overall_end_time=('end_time', 'max'),
    overall_mode_spindle_speed=('most_common_spindle_speed', lambda x: x.mode().iloc[0] if not x.mode().empty else 0),
    overall_mode_feed_rate=('most_common_feed_rate', lambda x: x.mode().iloc[0] if not x.mode().empty else 0),
    overall_running_duration_sum=('duration_seconds', 'sum') # Total Running dari sub-program
).reset_index()

# 2. Inisialisasi daftar kosong untuk menyimpan semua sesi program induk yang terdeteksi
all_program_induk_sessions = []

# --- Konfigurasi Ambang Batas Jeda Sesi (dalam detik) ---
# Jeda yang lebih pendek dari ini akan dianggap bagian dari sesi yang sama.
# Sesuaikan nilai ini sesuai dengan definisi "interupsi" Anda.
SESSION_GAP_THRESHOLD_SECONDS = 300 # Contoh: 5 menit (300 detik).

def get_status_category_for_loss(status):
    if status in IDLE_STATUSES: return 'IDLE'
    if status in OTHER_STATUSES: return 'OTHER'
    return 'RUNNING_OR_OTHER_KNOWN'

def is_standard_program(program_name):
    if program_name and isinstance(program_name, str) and program_name.strip().upper().startswith('N'):
        return True
    return False

for _, row_induk_summary in temp_df_induk_summary.iterrows():
    program_main_name = row_induk_summary['program_main_name']
    overall_start_time_induk = row_induk_summary['program_induk_overall_start_time']
    overall_end_time_induk = row_induk_summary['program_induk_overall_end_time']
    
    overall_spindle_mode = row_induk_summary['overall_mode_spindle_speed']
    overall_feed_mode = row_induk_summary['overall_mode_feed_rate']
    overall_running_sum = row_induk_summary['overall_running_duration_sum']

    #st.subheader(f"DEBUG: Memproses Program Induk: {program_main_name}")

    # relevant_logs_from_db2_raw = get_program_report_from_db2(
    #     selected_machine,
    #     overall_start_time_induk.date(),
    #     overall_end_time_induk.date(),
    #     program_main_name
    # )

    # --- PERBAIKAN DI SINI: Gunakan fungsi yang di-cache ---
    relevant_logs_from_db2_raw = cached_get_program_report_from_db2(
        selected_machine,
        overall_start_time_induk.date(),
        overall_end_time_induk.date(),
        program_main_name
    )
    # --- AKHIR PERBAIKAN ---

    #st.write(f"DEBUG: Hasil dari get_program_report_from_db2 untuk {program_main_name}:")
    if relevant_logs_from_db2_raw:
        df_relevant_logs_from_db2 = pd.DataFrame(relevant_logs_from_db2_raw)
        #st.dataframe(df_relevant_logs_from_db2)
        #st.write("Kolom di df_relevant_logs_from_db2:", df_relevant_logs_from_db2.columns.tolist())
    else:
        st.info(f"Tidak ada log relevan dari get_program_report_from_db2 untuk {program_main_name}")

    if relevant_logs_from_db2_raw:
        logs_in_overall_window = pd.DataFrame(relevant_logs_from_db2_raw)
        
        logs_in_overall_window['timestamp_log'] = pd.to_datetime(logs_in_overall_window['timestamp'], utc=True)
        
        logs_in_overall_window['program_main_name_from_log'] = logs_in_overall_window['current_program'].apply(
            lambda x: str(x).split('-')[0].strip() if x and '-' in str(x) else None
        )
        logs_in_overall_window['status_category'] = logs_in_overall_window['status_text'].apply(get_status_category_for_loss)
        logs_in_overall_window = logs_in_overall_window.sort_values(by='timestamp_log').copy()

        #st.write(f"DEBUG: logs_in_overall_window (setelah parsing & kategorisasi) for {program_main_name}:")
        #st.dataframe(logs_in_overall_window)
        #st.write("Kolom di logs_in_overall_window:", logs_in_overall_window.columns.tolist())

        if not logs_in_overall_window.empty:
            cols_to_carry = [col for col in logs_in_overall_window.columns if col != 'timestamp_log']
            synthetic_end_row_data = {col: logs_in_overall_window[col].iloc[-1] for col in cols_to_carry}
            synthetic_end_row_data['timestamp_log'] = overall_end_time_induk
            synthetic_end_row = pd.DataFrame([synthetic_end_row_data])
            synthetic_end_row['timestamp_log'] = pd.to_datetime(synthetic_end_row['timestamp_log'], utc=True)

            combined_logs_for_induk_calc = pd.concat([logs_in_overall_window, synthetic_end_row])
            combined_logs_for_induk_calc = combined_logs_for_induk_calc.sort_values('timestamp_log').drop_duplicates(subset=['timestamp_log'], keep='first').copy()
            
            combined_logs_for_induk_calc['timestamp_log'] = pd.to_datetime(combined_logs_for_induk_calc['timestamp_log'], utc=True)
            combined_logs_for_induk_calc['next_timestamp_log'] = combined_logs_for_induk_calc['timestamp_log'].shift(-1)
            combined_logs_for_induk_calc['duration_segment'] = (combined_logs_for_induk_calc['next_timestamp_log'] - combined_logs_for_induk_calc['timestamp_log']).dt.total_seconds()
            
            combined_logs_for_induk_calc = combined_logs_for_induk_calc.dropna(subset=['next_timestamp_log']).copy()

            #st.write(f"DEBUG: combined_logs_for_induk_calc (setelah segmentasi) for {program_main_name}:")
            #st.dataframe(combined_logs_for_induk_calc)
            #st.write("Kolom di combined_logs_for_induk_calc:", combined_logs_for_induk_calc.columns.tolist())

            # --- LOGIKA FINAL DETEKSI SESI & PENCATATAN NOTES ---
            current_session_start_time = None
            current_session_total_process_time = 0.0
            current_session_loss_time = 0.0
            current_session_notes = []
            is_main_program_active_in_this_session = False
            
            # PERBAIKAN: Inisialisasi list ini di sini untuk setiap program induk yang diproses
            detected_sessions_for_current_program_induk = []

            for idx_seg, segment_row in combined_logs_for_induk_calc.iterrows():
                segment_start_time = segment_row['timestamp_log']
                segment_end_time = segment_row['next_timestamp_log']
                segment_duration = segment_row['duration_segment']
                segment_status_category = segment_row['status_category']
                segment_program_main_name_from_log = segment_row['program_main_name_from_log']
                segment_current_program_value = str(segment_row['current_program']).strip() if segment_row['current_program'] is not None else ""

                is_current_segment_this_main_program_running = \
                    (program_main_name == segment_program_main_name_from_log) and \
                    (segment_status_category == 'RUNNING_OR_OTHER_KNOWN') and \
                    is_standard_program(segment_current_program_value)
                
                is_current_segment_other_standard_program_running = \
                    (program_main_name != segment_program_main_name_from_log) and \
                    (segment_status_category == 'RUNNING_OR_OTHER_KNOWN') and \
                    is_standard_program(segment_current_program_value)

                # --- Skenario 1: Program Induk Aktif Terdeteksi ---
                if is_current_segment_this_main_program_running:
                    if not is_main_program_active_in_this_session: # Program Induk baru saja dimulai/dilanjutkan
                        current_session_start_time = segment_start_time
                        current_session_total_process_time = 0.0
                        current_session_loss_time = 0.0
                        current_session_notes = []
                        
                        if len(detected_sessions_for_current_program_induk) > 0: # Ini adalah sesi lanjutan dalam program induk ini
                            current_session_notes.append(f"Lanjutan (Waktu: {current_session_start_time.tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')})")
                        else: # Ini adalah sesi pertama untuk program induk ini
                            current_session_notes.append(f"Mulai Sesi (Waktu: {current_session_start_time.tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')})")
                        
                        is_main_program_active_in_this_session = True
                    
                    # Akumulasi durasi ke sesi saat ini
                    current_session_total_process_time += segment_duration
                    if segment_status_category == 'IDLE' or segment_status_category == 'OTHER' or not is_standard_program(segment_current_program_value):
                        current_session_loss_time += segment_duration

                # --- Skenario 2: Program Lain Aktif (Interupsi yang Jelas) ---
                elif is_current_segment_other_standard_program_running:
                    if is_main_program_active_in_this_session: # Program Induk aktif diinterupsi
                        current_session_end_time = segment_start_time
                        
                        detected_sessions_for_current_program_induk.append({
                            'program_main_name': program_main_name,
                            'session_start_time': current_session_start_time,
                            'session_end_time': current_session_end_time,
                            'total_process_time_seconds': current_session_total_process_time,
                            'total_loss_time_seconds': current_session_loss_time,
                            'notes': "; ".join(current_session_notes) + f"; Interupsi oleh '{segment_current_program_value}' (Waktu: {segment_start_time.tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')})"
                        })
                        
                        # Reset state
                        is_main_program_active_in_this_session = False
                        current_session_start_time = None
                        current_session_total_process_time = 0.0
                        current_session_loss_time = 0.0
                        current_session_notes = []
                    # Program lain yang aktif ini tidak memulai sesi baru untuk program_main_name
                
                # --- Skenario 3: Jeda (Idle/Other/Non-Standar Program Seperti MDI.PRG) ---
                else: # segment_status_category is IDLE or OTHER, OR program is non-standard (e.g., MDI.PRG)
                    if is_main_program_active_in_this_session: # Jika Program Induk sedang aktif di sesi ini
                        # Cek apakah jeda ini melebihi ambang batas
                        # Jeda adalah dari EndTime segmen terakhir yang diakumulasi HINGGA StartTime segmen ini
                        # Diasumsikan logs diurutkan, jadi segment_start_time - (current_session_start_time + accumulated_duration)
                        
                        # total_duration_so_far = (segment_start_time - current_session_start_time).total_seconds()
                        # gap_duration = total_duration_so_far - current_session_total_process_time
                        
                        # Untuk lebih akurat, gunakan langsung dari previous end time jika memungkinkan,
                        # atau paling tidak, durasi segmen itu sendiri
                        
                        if segment_duration > SESSION_GAP_THRESHOLD_SECONDS:
                            # Jeda panjang, sesi program induk berakhir
                            current_session_end_time = segment_start_time
                            all_program_induk_sessions.append({
                                'program_main_name': program_main_name,
                                'session_start_time': current_session_start_time,
                                'session_end_time': current_session_end_time,
                                'total_process_time_seconds': current_session_total_process_time,
                                'total_loss_time_seconds': current_session_loss_time,
                                'notes': "; ".join(current_session_notes) + f"; Jeda Panjang Terdeteksi (Durasi: {format_seconds_to_hhmmss(segment_duration)}, Waktu: {segment_start_time.tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')})"
                            })
                            # Reset state
                            is_main_program_active_in_this_session = False
                            current_session_start_time = None
                            current_session_total_process_time = 0.0
                            current_session_loss_time = 0.0
                            current_session_notes = []
                        else:
                            # Jeda pendek, masih bagian dari sesi yang sama, hanya menambah loss
                            current_session_total_process_time += segment_duration
                            current_session_loss_time += segment_duration
                    # Jika tidak aktif, dan ini adalah jeda, tidak ada yang perlu dilakukan karena tidak ada sesi program induk aktif

            # --- Setelah loop segmen selesai, tambahkan sesi Program Induk terakhir yang belum ditutup ---
            if is_main_program_active_in_this_session and current_session_start_time is not None:
                # Sesi terakhir berakhir secara normal di akhir rentang keseluruhan
                detected_sessions_for_current_program_induk.append({
                    'program_main_name': program_main_name,
                    'session_start_time': current_session_start_time,
                    'session_end_time': overall_end_time_induk,
                    'total_process_time_seconds': current_session_total_process_time,
                    'total_loss_time_seconds': current_session_loss_time,
                    'notes': "; ".join(current_session_notes) + f"; Selesai Normal (Waktu: {overall_end_time_induk.tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')})"
                })
            
            # --- Tambahkan sesi ke daftar global all_program_induk_sessions ---
            # Pastikan ini hanya dijalankan sekali per program_main_name, dan di luar loop segment
            all_program_induk_sessions.extend(detected_sessions_for_current_program_induk)

            # Jika setelah semua pemrosesan, tidak ada sesi terdeteksi yang aktif (misal seluruh span cuma idle/other/makro)
            if not detected_sessions_for_current_program_induk and (overall_end_time_induk - overall_start_time_induk).total_seconds() > 0:
                all_program_induk_sessions.append({
                    'program_main_name': program_main_name,
                    'session_start_time': overall_start_time_induk,
                    'session_end_time': overall_end_time_induk,
                    'total_process_time_seconds': 0.0,
                    'total_loss_time_seconds': (overall_end_time_induk - overall_start_time_induk).total_seconds(),
                    'notes': "Tidak ada aktivitas Running Program Induk (N-standard) yang terdeteksi, hanya status non-Running atau program non-standar."
                })

        else: # Jika logs_in_overall_window kosong untuk program ini
            all_program_induk_sessions.append({
                'program_main_name': program_main_name,
                'session_start_time': overall_start_time_induk,
                'session_end_time': overall_end_time_induk,
                'total_process_time_seconds': 0.0,
                'total_loss_time_seconds': (overall_end_time_induk - overall_start_time_induk).total_seconds(),
                'notes': "Tidak ada log detail dalam rentang Program Induk."
            })
    else: # Jika relevant_logs_from_db2_raw kosong
          all_program_induk_sessions.append({
            'program_main_name': program_main_name,
            'session_start_time': overall_start_time_induk,
            'session_end_time': overall_end_time_induk,
            'total_process_time_seconds': 0.0,
            'total_loss_time_seconds': (overall_end_time_induk - overall_start_time_induk).total_seconds(),
            'notes': "Tidak ada log status yang ditemukan untuk Program Induk ini."
        })

# Setelah loop utama selesai, buat df_program_induk yang baru dari semua sesi
df_program_induk = pd.DataFrame(all_program_induk_sessions)

if df_program_induk.empty:
    st.info("Tidak ada data Program Induk yang ditemukan setelah analisis sesi.")
    st.stop()

# Lanjutkan perhitungan metrik dan tampilan untuk df_program_induk yang baru direkonstruksi
df_program_induk['session_start_time'] = pd.to_datetime(df_program_induk['session_start_time'], utc=True)
df_program_induk['session_end_time'] = pd.to_datetime(df_program_induk['session_end_time'], utc=True)

# Inisialisasi kolom yang mungkin belum ada (untuk robustness)
if 'target_duration_induk_seconds' not in df_program_induk.columns: df_program_induk['target_duration_induk_seconds'] = 0.0
if 'notes_induk' not in df_program_induk.columns: df_program_induk['notes_induk'] = ""

#!!!!!
# --- BAGIAN BARU: Input Quantity & Target Durasi Per Program Induk ---
st.subheader("Set Quantity")

# Siapkan DataFrame untuk editor
# Cukup ambil kolom yang relevan dari df_program_induk
df_sessions_for_editor = df_program_induk[[
    'program_main_name',
    'session_start_time',
    'session_end_time'
]].copy()
df_sessions_for_editor['Start Time'] = df_sessions_for_editor['session_start_time'].dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')
df_sessions_for_editor['End Time'] = df_sessions_for_editor['session_end_time'].dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')


# Inisialisasi/perbarui session_state untuk input manual per sesi
state_key_prefix = f"induk_editor_{selected_machine}_{start_date}_{end_date}_{selected_main_program_input}"
if 'editable_session_data' not in st.session_state or \
   st.session_state.editable_session_data_key != state_key_prefix:

    st.session_state.editable_session_data = []
    st.session_state.editable_session_data_key = state_key_prefix

    for _, row in df_sessions_for_editor.iterrows():
        session_id = f"{row['program_main_name']}_{row['Start Time']}"
        quantity = st.session_state.get(f"quantity_session_{session_id}", 1)
        notes = st.session_state.get(f"notes_session_{session_id}", "")
        st.session_state.editable_session_data.append({
            'program_main_name': row['program_main_name'],
            'Start Time': row['Start Time'],
            'End Time': row['End Time'],
            'Quantity': int(quantity),
            'Catatan': str(notes)
        })
    df_editable_sessions = pd.DataFrame(st.session_state.editable_session_data)
else:
    df_editable_sessions = pd.DataFrame(st.session_state.editable_session_data)

edited_df_sessions = st.data_editor(
    df_editable_sessions,
    key="program_session_editor",
    use_container_width=True,
    num_rows="fixed", # Tidak mengizinkan penambahan baris baru
    column_config={
        "program_main_name": st.column_config.TextColumn("Main Program", disabled=True),
        "Start Time": st.column_config.TextColumn("Start Time", disabled=True),
        "End Time": st.column_config.TextColumn("End Time", disabled=True),
        "Quantity": st.column_config.NumberColumn("Quantity", min_value=1, format="%d"),
        "Catatan": st.column_config.TextColumn("Catatan", width="large")
    }
)

# Tangkap editan dan perbarui session_state untuk setiap sesi
if st.session_state.program_session_editor and edited_df_sessions is not None:
    for row_index, edited_values in st.session_state.program_session_editor['edited_rows'].items():
        # Dapatkan ID sesi yang unik dari DataFrame yang sudah diedit
        session_id_from_editor = f"{edited_df_sessions.loc[row_index, 'program_main_name']}_{edited_df_sessions.loc[row_index, 'Start Time']}"
        
        if 'Quantity' in edited_values:
            st.session_state[f"quantity_session_{session_id_from_editor}"] = edited_values['Quantity']
        if 'Catatan' in edited_values:
            st.session_state[f"notes_session_{session_id_from_editor}"] = edited_values['Catatan']

# --- GABUNGKAN INPUT MANUAL PER SESI KE DF UTAMA df_program_induk ---
# Tambahkan Quantity dari input manual ke df_program_induk
df_program_induk['Quantity'] = df_program_induk.apply(
    lambda row: st.session_state.get(f"quantity_session_{row['program_main_name']}_{row['session_start_time'].tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')}", 1),
    axis=1
)
# Perbarui notes_induk dari input manual
# df_program_induk['notes_induk'] = df_program_induk.apply(
#     lambda row: st.session_state.get(f"notes_session_{row['program_main_name']}_{row['session_start_time'].tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')}", ""),
#     axis=1
# )

df_program_induk['Catatan'] = df_program_induk.apply(
    lambda row: st.session_state.get(f"notes_session_{row['program_main_name']}_{row['session_start_time'].tz_convert(local_tz).strftime('%Y-%m-%d %H:%M:%S')}", ""),
    axis=1
)

for idx, row in df_program_induk.iterrows():
    p_name = row['program_main_name']
    
    target_minutes = st.session_state.get(f"target_induk_minutes_{p_name}", 0.0)
    target_notes = st.session_state.get(f"notes_induk_{p_name}", "")

    df_program_induk.loc[idx, 'target_duration_induk_seconds'] = float(target_minutes) * 60
    
    session_notes = df_program_induk.loc[idx, 'notes']
    final_notes = []
    if session_notes and session_notes.strip() and session_notes != "Tidak ada log detail dalam rentang Program Induk." and session_notes != "Tidak ada aktivitas Running Program Induk (N-standard) yang terdeteksi, hanya status non-Running atau program non-standar.":
        final_notes.append(session_notes)
    if target_notes and target_notes.strip():
        final_notes.append(f"Catatan Manual: {target_notes}")
    
    df_program_induk.loc[idx, 'notes_induk'] = "; ".join(final_notes) if final_notes else ""

    original_summary_row = temp_df_induk_summary[temp_df_induk_summary['program_main_name'] == p_name]
    if not original_summary_row.empty:
        df_program_induk.loc[idx, 'most_common_spindle_speed'] = original_summary_row['overall_mode_spindle_speed'].iloc[0]
        df_program_induk.loc[idx, 'most_common_feed_rate'] = original_summary_row['overall_mode_feed_rate'].iloc[0]
        df_program_induk.loc[idx, 'total_actual_running_duration_seconds'] = original_summary_row['overall_running_duration_sum'].iloc[0]
    else:
        df_program_induk.loc[idx, 'most_common_spindle_speed'] = 0
        df_program_induk.loc[idx, 'most_common_feed_rate'] = 0
        df_program_induk.loc[idx, 'total_actual_running_duration_seconds'] = 0.0


df_program_induk['Start Time'] = df_program_induk['session_start_time'].dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')
df_program_induk['End Time'] = df_program_induk['session_end_time'].dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')

df_program_induk['target_duration_induk_hhmmss'] = df_program_induk['target_duration_induk_seconds'].apply(format_seconds_to_hhmmss)
df_program_induk['overall_duration_hhmmss'] = df_program_induk['total_process_time_seconds'].apply(format_seconds_to_hhmmss)
df_program_induk['loss_time_hhmmss'] = df_program_induk['total_loss_time_seconds'].apply(format_seconds_to_hhmmss)
df_program_induk['cycle_time_seconds'] = df_program_induk['total_process_time_seconds'] - df_program_induk['total_loss_time_seconds']
df_program_induk['cycle_time_hhmmss'] = df_program_induk['cycle_time_seconds'].apply(format_seconds_to_hhmmss)

df_program_induk['overall_duration_per_piece_seconds'] = df_program_induk.apply(
    lambda row: row['total_process_time_seconds'] / row['Quantity'] if row['Quantity'] > 0 else 0,
    axis=1
)
df_program_induk['loss_time_per_piece_seconds'] = df_program_induk.apply(
    lambda row: row['total_loss_time_seconds'] / row['Quantity'] if row['Quantity'] > 0 else 0,
    axis=1
)
df_program_induk['cycle_time_per_piece_seconds'] = df_program_induk.apply(
    lambda row: row['cycle_time_seconds'] / row['Quantity'] if row['Quantity'] > 0 else 0,
    axis=1
)

# Format kolom-kolom baru ke HH:MM:SS
df_program_induk['overall_duration_per_piece_hhmmss'] = df_program_induk['overall_duration_per_piece_seconds'].apply(format_seconds_to_hhmmss)
df_program_induk['loss_time_per_piece_hhmmss'] = df_program_induk['loss_time_per_piece_seconds'].apply(format_seconds_to_hhmmss)
df_program_induk['cycle_time_per_piece_hhmmss'] = df_program_induk['cycle_time_per_piece_seconds'].apply(format_seconds_to_hhmmss)

Total_overall_duration_seconds = df_program_induk['total_process_time_seconds'].sum()
Total_overall_duration_hhmmss = format_seconds_to_hhmmss(Total_overall_duration_seconds)

Total_loss_time_seconds = df_program_induk['total_loss_time_seconds'].sum()
Total_loss_time_hhmmss = format_seconds_to_hhmmss(Total_loss_time_seconds)

Total_cycle_time_seconds = df_program_induk['cycle_time_seconds'].sum()
Total_cycle_time_hhmmss = format_seconds_to_hhmmss(Total_cycle_time_seconds)

Total_overall_duration_per_piece_seconds = df_program_induk['overall_duration_per_piece_seconds'].sum()
Total_overall_duration_per_piece_hhmmss = format_seconds_to_hhmmss(Total_overall_duration_per_piece_seconds)

Total_loss_time_per_piece_seconds = df_program_induk['loss_time_per_piece_seconds'].sum()
Total_loss_time_per_piece_hhmmss = format_seconds_to_hhmmss(Total_loss_time_per_piece_seconds)

Total_cycle_time_per_pieces_seconds = df_program_induk['cycle_time_per_piece_seconds'].sum()
Total_cycle_time_per_pieces_hhmmss = format_seconds_to_hhmmss(Total_cycle_time_per_pieces_seconds)

df_program_induk['efficiency_percent'] = df_program_induk.apply(
    lambda row: min(100.0, (row['target_duration_induk_seconds'] / row['total_process_time_seconds']) * 100)
    if row['total_process_time_seconds'] > 0 else 0.0,
    axis=1
)
df_program_induk['efficiency_percent'] = df_program_induk['efficiency_percent'].round(2)
df_program_induk['efficiency_status'] = df_program_induk['efficiency_percent'].apply(classify_efficiency)


#st.subheader("DEBUG: df_program_induk (setelah semua perhitungan dan rekonstruksi)")
#st.dataframe(df_program_induk)
#st.write("Kolom di df_program_induk (setelah semua perhitungan):", df_program_induk.columns.tolist())

st.markdown("#### Main Program Cycle Time")

# Buat DataFrame untuk tampilan dengan kolom yang diinginkan
df_program_induk_display = df_program_induk[[
    'program_main_name',
    'Start Time',
    'End Time',
    'overall_duration_hhmmss',
    'loss_time_hhmmss',
    'cycle_time_hhmmss',
    'Quantity',
    'overall_duration_per_piece_hhmmss',
    'loss_time_per_piece_hhmmss',
    'cycle_time_per_piece_hhmmss',
    'notes_induk',
    'Catatan'
]].rename(columns={
    'program_main_name': 'Main Program',
    'overall_duration_hhmmss': 'Duration',
    'loss_time_hhmmss': 'Loss Time',
    'cycle_time_hhmmss': 'Cutting Time',
    'Quantity': 'Qty',
    'overall_duration_per_piece_hhmmss': 'Duration/Pcs',
    'loss_time_per_piece_hhmmss': 'Loss Time/Pcs',
    'cycle_time_per_piece_hhmmss': 'Cutting Time/Pcs',
    'notes_induk': 'Note',
    'Catatan': 'Note Qty'
})

st.dataframe(df_program_induk_display, use_container_width=True)

# Tampilkan baris total secara terpisah
st.markdown("##### Total Durations (All Sessions)")
df_total_induk = pd.DataFrame([{
    'Total Duration': Total_overall_duration_hhmmss,
    'Total Loss Time': Total_loss_time_hhmmss,
    'Total Cutting Time': Total_cycle_time_hhmmss,
    'Total Duration/pcs': Total_overall_duration_per_piece_hhmmss,
    'Total Loss Time/pcs': Total_loss_time_per_piece_hhmmss,
    'Total Cutting Time/Pcs': Total_cycle_time_per_pieces_hhmmss
}])

st.dataframe(df_total_induk, use_container_width=True, hide_index=True)

# --- Tambahan Baru: Tabel Total Efisiensi ---
st.markdown("##### Overall Efficiency")

# Hitung efisiensi total
Overall_efficiency_percent_induk = min(100.0, (Total_target_duration_seconds / Total_overall_duration_per_piece_seconds) * 100) if Total_overall_duration_per_piece_seconds > 0 else 0.0
Overall_efficiency_status_induk = classify_efficiency(Overall_efficiency_percent_induk)

# Buat DataFrame untuk tabel efisiensi
df_total_efficiency_induk = pd.DataFrame([{
    'Total Target Duration': Total_target_duration_hhmmss,
    'Total Actual Duration': Total_overall_duration_per_piece_hhmmss,
    'Overall Efficiency (%)': round(Overall_efficiency_percent_induk, 2),
    'Efficiency Status': Overall_efficiency_status_induk
}])

st.dataframe(df_total_efficiency_induk, use_container_width=True, hide_index=True)

loss_data = []
df_loss_breakdown = pd.DataFrame()
loss_data_per_piece = []
df_loss_breakdown_per_piece = pd.DataFrame()

# Periksa jika ada total loss time untuk membuat data
if Total_loss_time_seconds > 0:
    relevant_sessions = df_program_induk[df_program_induk['total_loss_time_seconds'] > 0]
    total_quantity_all_sessions = df_program_induk['Quantity'].sum()
    
    # Jika combined_logs_for_induk_calc ada dari proses sebelumnya, gunakan itu
    if 'combined_logs_for_induk_calc' in locals():
        for _, row in relevant_sessions.iterrows():
            program_main_name = row['program_main_name']
            session_start = row['session_start_time']
            session_end = row['session_end_time']
            session_quantity = row['Quantity']

            logs_in_session = combined_logs_for_induk_calc[
                (combined_logs_for_induk_calc['program_main_name_from_log'] == program_main_name) &
                (combined_logs_for_induk_calc['timestamp_log'] >= session_start) &
                (combined_logs_for_induk_calc['next_timestamp_log'] <= session_end)
            ].copy()
            
            loss_logs = logs_in_session[
                logs_in_session['status_text'].isin(IDLE_STATUSES + OTHER_STATUSES) |
                (~logs_in_session['current_program'].apply(is_standard_program))
            ].copy()

            if not loss_logs.empty:
                loss_summary = loss_logs.groupby('status_text')['duration_segment'].sum().reset_index()
                for _, loss_row in loss_summary.iterrows():
                    loss_data.append({
                        'Category': loss_row['status_text'],
                        'Duration (seconds)': loss_row['duration_segment']
                    })
                    
                    if total_quantity_all_sessions > 0 and session_quantity > 0:
                        loss_data_per_piece.append({
                            'Category': loss_row['status_text'],
                            'Duration (seconds)': loss_row['duration_segment']
                        })

# Agregasi data loss jika ada
if loss_data:
    df_loss_breakdown = pd.DataFrame(loss_data)
    df_loss_breakdown = df_loss_breakdown.groupby('Category')['Duration (seconds)'].sum().reset_index()

if loss_data_per_piece:
    df_loss_breakdown_per_piece = pd.DataFrame(loss_data_per_piece)
    df_loss_breakdown_per_piece = df_loss_breakdown_per_piece.groupby('Category')['Duration (seconds)'].sum().reset_index()
    # Konversi total durasi menjadi durasi per pieces
    if total_quantity_all_sessions > 0:
        df_loss_breakdown_per_piece['Duration (seconds)'] = df_loss_breakdown_per_piece['Duration (seconds)'] / total_quantity_all_sessions
    else:
        df_loss_breakdown_per_piece = pd.DataFrame() # Kosongkan jika total qty 0

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
    if not df_loss_breakdown.empty:
        fig_loss_pie = px.pie(
            df_loss_breakdown,
            values='Duration (seconds)',
            names='Category',
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
    if not df_loss_breakdown_per_piece.empty:
        fig_loss_pie_per_piece = px.pie(
            df_loss_breakdown_per_piece,
            values='Duration (seconds)',
            names='Category',
            title=f"Loss Time/Pcs Breakdown",
            hole=0.4
        )
        fig_loss_pie_per_piece.update_traces(textinfo='percent+label')
        fig_loss_pie_per_piece.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        st.plotly_chart(fig_loss_pie_per_piece, use_container_width=True)
    else:
        st.info("Tidak ada data loss time per pieces.")

# --- BARU: Tabel Detail Loss Time ---
st.markdown(f"#### Detail Waktu Loss untuk Program Induk yang Terpilih: {selected_main_program_input if selected_main_program_input else 'N/A'}")

# Dapatkan sesi-sesi yang tersedia untuk program induk yang difilter
current_main_program_sessions = df_program_induk[
    df_program_induk['program_main_name'] == selected_main_program_input
].copy()

if not current_main_program_sessions.empty:
    # Buat string representasi sesi untuk selectbox
    session_options = []
    for idx, row in current_main_program_sessions.iterrows():
        session_label = f"Sesi: {row['Start Time']} - {row['End Time']} ({row['notes_induk']})"
        session_options.append((session_label, idx)) # Simpan index baris untuk pengambilan data

    # Tambahkan opsi "Pilih Semua Sesi"
    session_options.insert(0, ("Semua Sesi", "ALL"))

    selected_session_label = st.selectbox(
        "Pilih Sesi untuk Detail Waktu Loss",
        options=[opt[0] for opt in session_options],
        format_func=lambda x: x # Tampilkan labelnya saja
    )
    
    # Dapatkan index sesi yang dipilih
    selected_session_idx = next((opt[1] for opt in session_options if opt[0] == selected_session_label), None)

    if selected_session_idx is not None:
        if selected_session_idx == "ALL":
            st.info(f"Menampilkan detail waktu loss untuk **semua sesi** Program Induk: **{selected_main_program_input}**")
            # Gunakan combined_logs_for_induk_calc yang sudah ada untuk keseluruhan program induk
            # Ini sudah difilter untuk selected_main_program_input dari awal loop
            df_loss_to_display = combined_logs_for_induk_calc.copy()
            # Pastikan hanya menampilkan log yang relevan dengan loss (idle, other, atau non-standard program)
            df_loss_to_display = df_loss_to_display[
                (df_loss_to_display['status_category'].isin(['IDLE', 'OTHER'])) |
                (~df_loss_to_display['current_program'].apply(is_standard_program) &
                 (df_loss_to_display['status_category'] == 'RUNNING_OR_OTHER_KNOWN'))
            ].copy()

        else:
            st.info(f"Menampilkan detail waktu loss untuk **sesi yang dipilih** Program Induk: **{selected_main_program_input}**")
            # Ambil detail sesi yang dipilih
            chosen_session = current_main_program_sessions.loc[selected_session_idx]
            session_start_dt = chosen_session['session_start_time']
            session_end_dt = chosen_session['session_end_time']

            # Filter combined_logs_for_induk_calc berdasarkan rentang waktu sesi yang dipilih
            df_loss_to_display = combined_logs_for_induk_calc[
                (combined_logs_for_induk_calc['timestamp_log'] >= session_start_dt) &
                (combined_logs_for_induk_calc['timestamp_log'] < session_end_dt) # Gunakan < end_dt untuk menghindari duplikasi end_time
            ].copy()

            # Filter untuk segmen yang relevan dengan loss
            df_loss_to_display = df_loss_to_display[
                (df_loss_to_display['status_category'].isin(['IDLE', 'OTHER'])) |
                (~df_loss_to_display['current_program'].apply(is_standard_program) &
                 (df_loss_to_display['status_category'] == 'RUNNING_OR_OTHER_KNOWN'))
            ].copy()
        
        # --- Lanjutkan menampilkan tabel loss ---
        if not df_loss_to_display.empty:
            df_loss_to_display['Start Time Segment'] = df_loss_to_display['timestamp_log'].dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_loss_to_display['End Time Segment'] = df_loss_to_display['next_timestamp_log'].dt.tz_convert(local_tz).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_loss_to_display['Durasi Loss (HH:MM:SS)'] = df_loss_to_display['duration_segment'].apply(format_seconds_to_hhmmss)

            total_duration_loss_seconds = df_loss_to_display['duration_segment'].sum()
            total_duration_loss_hhmmss = format_seconds_to_hhmmss(total_duration_loss_seconds)

            total_row = pd.DataFrame([{
                'status_text': 'TOTAL',
                'Start Time Segment': '',
                'End Time Segment': '',
                'Durasi Loss (HH:MM:SS)': total_duration_loss_hhmmss,
                'current_program': ''
            }])
            
            df_detailed_loss_for_display = pd.concat([df_loss_to_display, total_row], ignore_index=True)

            st.dataframe(df_detailed_loss_for_display[[
                'status_text',
                'Start Time Segment',
                'End Time Segment',
                'Durasi Loss (HH:MM:SS)',
                'current_program'
            ]].rename(columns={
                'status_text': 'Status',
                'current_program': 'Program Saat Loss',
                'Durasi Loss (HH:MM:SS)': 'Durasi Loss (HH:MM:SS)'
            }), use_container_width=True)
        else:
            st.info(f"Tidak ada detail waktu loss yang tercatat untuk Program Induk '{selected_main_program_input}' dalam rentang waktu yang dipilih.")
    else:
        st.info("Pilih sesi dari daftar untuk melihat detail waktu loss.")
else:
    st.info(f"Tidak ada sesi program induk yang terdeteksi untuk '{selected_main_program_input}'. Mohon pilih Program Induk lain dari sidebar.")

st.markdown("#### Raw Data")
st.write(df_program)


# --- Modifikasi dimulai dari sini ---
start_date_report = df_program_induk['Start Time'].min()

st.markdown("---")
st.header("Simpan Laporan Analisa")
st.info(f"Gunakan tombol di bawah ini untuk menyimpan laporan analisis efisiensi program pada tanggal {start_date.strftime('%Y-%m-%d')}.")

if st.button("Save Report to Database"):
    # Cek dan simpan laporan sub-program
    sub_program_save_success = False
    if not df_efficiency.empty:
        with st.spinner(f"Saving sub-program report for {selected_machine} on {start_date}..."):
            sub_program_save_success = save_sub_program_analysis_report(selected_machine, start_date_report, df_efficiency)
            if sub_program_save_success:
                st.success(f"Laporan efisiensi sub-program untuk {selected_machine} pada tanggal {start_date} berhasil disimpan!")
            else:
                st.error(f"Gagal menyimpan laporan efisiensi sub-program untuk {selected_machine} pada tanggal {start_date}.")
    else:
        st.warning("Tidak ada data efisiensi sub-program yang valid untuk disimpan.")

    # Cek dan simpan laporan program induk
    main_program_save_success = False
    # --- PERBAIKAN DI SINI: Gunakan df_program_induk dari perhitungan sebelumnya ---
    if not df_program_induk.empty:
        with st.spinner(f"Saving main program report for {selected_machine} on {start_date}..."):
            main_program_save_success = save_main_program_analysis(selected_machine, start_date_report, df_program_induk)
            if main_program_save_success:
                st.success(f"Laporan analisis program induk untuk {selected_machine} pada tanggal {start_date} berhasil disimpan!")
            else:
                st.error(f"Gagal menyimpan laporan analisis program induk untuk {selected_machine} pada tanggal {start_date}.")
    else:
        st.warning("Tidak ada data analisis program induk yang valid untuk disimpan.")
    # --- AKHIR PERBAIKAN ---
    
    loss_breakdown_save_success = False
    if not df_loss_breakdown.empty:
        with st.spinner(f"Saving loss breakdown report for {selected_machine} on {start_date}..."):
            loss_breakdown_save_success = save_loss_breakdown_report(selected_machine, start_date_report, df_loss_breakdown)
            if loss_breakdown_save_success:
                st.success("Laporan rincian waktu loss berhasil disimpan!")
            else:
                st.error("Gagal menyimpan laporan rincian waktu loss.")
    else:
        st.warning("Tidak ada data rincian waktu loss untuk disimpan.")

    loss_breakdown_per_piece_save_success = False
    if not df_loss_breakdown_per_piece.empty:
        with st.spinner(f"Saving loss breakdown report for {selected_machine} on {start_date}..."):
            loss_breakdown_per_piece_save_success = save_loss_breakdown_per_piece_report(selected_machine, start_date_report, df_loss_breakdown_per_piece)
            if loss_breakdown_per_piece_save_success:
                st.success("Laporan rincian waktu loss per piece berhasil disimpan!")
            else:
                st.error("Gagal menyimpan laporan rincian waktu loss per piece.")
    else:
        st.warning("Tidak ada data rincian waktu loss per piece untuk disimpan.")

    if sub_program_save_success or main_program_save_success:
        st.cache_data.clear()
        st.rerun()