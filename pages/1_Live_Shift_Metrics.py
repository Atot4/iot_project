# pages/1_Live_Shift_Metrics.py

import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
from datetime import date, time as dt_time, timezone

import sys
import os
import logging
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app_core.config import (
    SHIFTS,
    RUNNING_STATUSES,
    IDLE_STATUSES,
    MACHINE_DISPLAY_ORDER,
    DB_CONFIG
)
from app_core.db_manager import (
    get_shift_metrics_table_name,
    format_seconds_to_hhmm,
    get_shift_metrics_from_db,
    init_db_pool,
    db_pool
)

st.markdown(
    """
    <style>
    /* Mengatur ukuran font untuk nilai metrik */
    div[data-testid="stMetricValue"] {
        font-size: 20px; /* Sesuaikan ukuran font yang Anda inginkan */
    }
    /* Mengatur ukuran font untuk label metrik (opsional) */
    div[data-testid="stMetricLabel"] p {
        font-size: 20px; /* Sesuaikan ukuran font yang Anda inginkan */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- PENTING: Inisialisasi DB Pool untuk aplikasi Streamlit ini ---
try:
    init_db_pool()
    if db_pool is None:
        st.error("ERROR: Database connection pool failed to initialize. Please check database configuration.")
        st.stop()
except Exception as e:
    logger.critical(f"Critical error during Streamlit DB pool initialization: {e}", exc_info=True)
    st.error(f"Critical error during database connection: {e}")
    st.stop()
# --- AKHIR: Inisialisasi DB Pool untuk aplikasi Streamlit ini ---

# --- Caching untuk fungsi pengambilan data DB ---
@st.cache_data(ttl=60) # Cache hasil selama 60 detik
def cached_get_shift_metrics_from_db(machine_name, shift_name, start_date, end_date):
    logger.info(f"[ShiftMetricsPage - Cache] Fetching fresh data for {machine_name}-{shift_name}-{start_date} from DB.")
    return get_shift_metrics_from_db(machine_name, shift_name, start_date, end_date)

# --- Helper Functions ---
def get_shift_boundaries_for_display(selected_date_obj: date, shift_name_to_find: str) -> dict or None:
    """
    Mengembalikan dictionary dengan start_dt dan end_dt (objek datetime) untuk shift tertentu pada tanggal tertentu.
    Catatan: datetime yang dikembalikan adalah naive.
    """
    for shift_name, (start_hour, end_hour) in SHIFTS.items():
        if shift_name == shift_name_to_find:
            # Create NAIVE datetime for shift start on the selected date
            shift_start = datetime.datetime.combine(selected_date_obj, datetime.time(start_hour, 0, 0))
            
            # Create NAIVE datetime for shift end. Handle shifts crossing midnight.
            if end_hour == 0: # If end_hour is 0, it means midnight of the next day
                shift_end = datetime.datetime.combine(selected_date_obj + datetime.timedelta(days=1), datetime.time(0, 0, 0))
            else: # Otherwise, it's on the same day
                shift_end = datetime.datetime.combine(selected_date_obj, datetime.time(end_hour, 0, 0))

            return {"start_dt": shift_start, "end_dt": shift_end}
    return None

# Konfigurasi halaman Streamlit
st.set_page_config(layout="wide")

st.title("Live Shift Metrics")

# --- Kontrol Filter di Sidebar ---
st.sidebar.header("Filter Mesin")
selected_machines_filter = st.sidebar.multiselect(
    "Select a Machine",
    options=MACHINE_DISPLAY_ORDER,
    default=MACHINE_DISPLAY_ORDER # Default: semua mesin terpilih
)
st.sidebar.markdown("---")

# Filter MACHINE_DISPLAY_ORDER berdasarkan pilihan di sidebar
if selected_machines_filter:
    machines_to_display = [m for m in MACHINE_DISPLAY_ORDER if m in selected_machines_filter]
else:
    machines_to_display = []

if not machines_to_display:
    st.info("Pilih setidaknya satu mesin dari sidebar untuk menampilkan metrik.")
    time.sleep(10)
    st.rerun()

# Dapatkan tanggal dan waktu saat ini (real-time)
current_date_today = date.today() # date object
now_local = datetime.datetime.now() # Naive datetime for current local time

# Determine current shift based on local time
current_shift_name = None
shift_boundaries_current_naive = None # This will hold naive local start/end datetimes from get_shift_boundaries_for_display

# Dynamically get local timezone or fall back to Asia/Jakarta
try:
    import tzlocal
    local_tz = tzlocal.get_localzone()
except ImportError:
    logger.warning("tzlocal not found. Assuming Asia/Jakarta timezone for local time conversion in Streamlit page.")
    class AsiaJakartaTZ(datetime.tzinfo):
        def utcoffset(self, dt): return datetime.timedelta(hours=7)
        def dst(self, dt): return datetime.timedelta(0)
        def tzname(self, dt): return "WIB"
    local_tz = AsiaJakartaTZ()

# Make current local time timezone-aware for comparison
now_local_aware = now_local.astimezone(local_tz)

# Iterate through defined shifts to find the current active shift
for sh_name, (start_hour, end_hour) in SHIFTS.items():
    # Create NAIVE datetime objects first (using get_shift_boundaries_for_display helper)
    temp_boundaries = get_shift_boundaries_for_display(current_date_today, sh_name)
    
    if temp_boundaries:
        # Convert NAIVE boundaries to AWARE for proper comparison with now_local_aware
        shift_start_aware_for_comparison = temp_boundaries['start_dt'].replace(tzinfo=local_tz)
        shift_end_aware_for_comparison = temp_boundaries['end_dt'].replace(tzinfo=local_tz)

        # Compare the current local aware time with the local aware shift boundaries
        if shift_start_aware_for_comparison <= now_local_aware < shift_end_aware_for_comparison:
            current_shift_name = sh_name
            # Store the AWARE boundaries, as these are needed for calculations later
            shift_boundaries_current_aware = {
                "start_dt": shift_start_aware_for_comparison, 
                "end_dt": shift_end_aware_for_comparison
            }
            break

if not current_shift_name:
    st.warning("Tidak ada shift yang sedang berjalan saat ini atau konfigurasi shift tidak mencakup waktu saat ini.")
    time.sleep(10)
    st.rerun()
else:
    # Display current shift info
    st.markdown(f"**{current_shift_name.replace('_', ' ').title()}** ({current_date_today.strftime('%Y-%m-%d')}), Last Updated {now_local.strftime('%H:%M:%S')}")
    
    # Determine the number of columns for display based on filtered machines
    num_columns_per_row_display = 1
    if len(machines_to_display) >= 2:
        num_columns_per_row_display = 2
    if len(machines_to_display) >= 4:
        num_columns_per_row_display = 3
    
    machine_chunks_display = [machines_to_display[i:i + num_columns_per_row_display] 
                              for i in range(0, len(machines_to_display), num_columns_per_row_display)]

    for chunk in machine_chunks_display:
        cols = st.columns(len(chunk))
        for i, machine_name in enumerate(chunk):
            with cols[i]:
                with st.container(border=True): 
                    st.subheader(f"{machine_name}")

                    if shift_boundaries_current_aware:
                        logger.debug(f"[ShiftMetricsPage] Machine: {machine_name}, Shift: {current_shift_name}, Date: {current_date_today}")
                        logger.debug(f"[ShiftMetricsPage] Shift boundaries for display (local aware): Start={shift_boundaries_current_aware['start_dt'].isoformat()}, End={shift_boundaries_current_aware['end_dt'].isoformat()}")

                        # Use the cached function to fetch shift metrics from DB
                        shift_metrics_db = cached_get_shift_metrics_from_db(
                            machine_name=machine_name,
                            shift_name=current_shift_name,
                            start_date=current_date_today, # Pass only the date object to db_manager
                            end_date=current_date_today    # Pass only the date object
                        )
                        
                        actual_metrics_for_display = None
                        if shift_metrics_db:
                            # Filter results from DB to ensure it matches the current shift name and date precisely
                            # This loop is important because get_shift_metrics_from_db might return multiple shifts for the day
                            # if your DB query wasn't precise enough, or if a shift crosses midnight.
                            # The 'shift_start_time' from DB will be UTC-aware.
                            for metric_entry in shift_metrics_db:
                                # Convert DB's shift_start_time (UTC) to local date for comparison with current_date_today (local date)
                                db_shift_start_local_date = metric_entry['shift_start_time'].astimezone(local_tz).date()
                                
                                if metric_entry['shift_name'] == current_shift_name and \
                                   db_shift_start_local_date == current_date_today:
                                    actual_metrics_for_display = metric_entry 
                                    logger.debug(f"[ShiftMetricsPage] Found matching metrics from DB: {actual_metrics_for_display}")
                                    break 

                        if actual_metrics_for_display:
                            current_time_aware_utc = datetime.datetime.now(timezone.utc)
                            
                            # Use shift_boundaries_current_aware for calculations (already UTC aware)
                            shift_start_dt_aware_for_calc = shift_boundaries_current_aware['start_dt'].astimezone(timezone.utc)
                            shift_end_dt_aware_for_calc = shift_boundaries_current_aware['end_dt'].astimezone(timezone.utc)
                            
                            logger.debug(f"[ShiftMetricsPage] Current time (UTC aware for calculations): {current_time_aware_utc.isoformat()}")
                            logger.debug(f"[ShiftMetricsPage] Shift boundaries (UTC aware for calculations): Start={shift_start_dt_aware_for_calc.isoformat()}, End={shift_end_dt_aware_for_calc.isoformat()}")

                            actual_elapsed_in_shift_sec = (current_time_aware_utc - shift_start_dt_aware_for_calc).total_seconds()
                            actual_elapsed_in_shift_sec = max(0.0, actual_elapsed_in_shift_sec)
                            
                            total_shift_slot_duration_sec = (shift_end_dt_aware_for_calc - shift_start_dt_aware_for_calc).total_seconds()
                            total_shift_slot_duration_sec = max(0.0, total_shift_slot_duration_sec)

                            runtime_sec = actual_metrics_for_display.get("runtime_seconds", 0.0)
                            idletime_sec = actual_metrics_for_display.get("idletime_seconds", 0.0)
                            other_time_sec = actual_metrics_for_display.get("other_time_seconds", 0.0)
                            
                            logger.debug(f"[ShiftMetricsPage] Raw metrics from DB: Runtime={runtime_sec}, Idletime={idletime_sec}, Other={other_time_sec}")
                            
                            total_tracked_time_from_db = runtime_sec + idletime_sec + other_time_sec
                            
                            unaccounted_time_sec = max(0.0, actual_elapsed_in_shift_sec - total_tracked_time_from_db)

                            sisa_waktu_shift_sec = max(0.0, total_shift_slot_duration_sec - actual_elapsed_in_shift_sec)
                            
                            logger.debug(f"[ShiftMetricsPage] Calculated values for pie chart: Runtime={runtime_sec}, Idletime={idletime_sec}, Other={other_time_sec}, Time Remaining={sisa_waktu_shift_sec}, Unaccounted Time={unaccounted_time_sec}")

                            pie_data = {
                                "Category": [],
                                "Value": []
                            }
                            if runtime_sec > 0.1: pie_data["Category"].append("Runtime"); pie_data["Value"].append(runtime_sec)
                            if idletime_sec > 0.1: pie_data["Category"].append("Idletime"); pie_data["Value"].append(idletime_sec)
                            if other_time_sec > 0.1: pie_data["Category"].append("Other Time"); pie_data["Value"].append(other_time_sec)
                            if unaccounted_time_sec > 0.1: pie_data["Category"].append("Unaccounted Time"); pie_data["Value"].append(unaccounted_time_sec)
                            if sisa_waktu_shift_sec > 0.1: pie_data["Category"].append("Time Remaining"); pie_data["Value"].append(sisa_waktu_shift_sec)
                            
                            if pie_data["Category"]: # Only if there's valid data to plot
                                df_pie = pd.DataFrame(pie_data)

                                # Display the DataFrame for debugging purposes (remove in production)
                                # st.write(f"DataFrame for {machine_name}:")
                                # st.dataframe(df_pie) # Use st.dataframe, assuming version compatibility is handled by updates.

                                fig = px.pie(
                                    df_pie,
                                    values="Value",
                                    names="Category",
                                    title=f"Shift Metrics for {machine_name}", # Changed title for better clarity
                                    color="Category",
                                    color_discrete_map={
                                        "Runtime": "#28A745",
                                        "Idletime": "#FFC107",
                                        "Other Time": "#6C757D",
                                        "Time Remaining": "#ADD8E6",
                                        "Unaccounted Time": "#DC3545"
                                    },
                                    hole=0.3,
                                )
                                fig.update_traces(
                                    textposition="inside", 
                                    textinfo="percent",
                                    hovertemplate="<b>%{label}</b><br>%{value:.0f} detik (%{percent})<extra></extra>"
                                )
                                fig.update_layout(
                                    margin=dict(l=0, r=0, t=80, b=0),
                                    height=300,
                                    showlegend=True,
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=-0.3,
                                        xanchor="center",
                                        x=0.5
                                    ),
                                    title_font_size=16
                                )
                                unique_chart_key = f"pie_chart_{machine_name}_{current_shift_name}_{current_date_today}"
                                st.plotly_chart(
                                    fig, use_container_width=True, key=unique_chart_key
                                )
                                logger.debug(f"[ShiftMetricsPage] Pie chart generated for {machine_name}-{current_shift_name}-{current_date_today}.")

                                # Display metrics in St.metric boxes below the chart
                                col_metrics = st.columns(3)
                                with col_metrics[0]:
                                    st.metric("Runtime", f"{format_seconds_to_hhmm(runtime_sec)}")
                                with col_metrics[1]:
                                    st.metric("Idletime", f"{format_seconds_to_hhmm(idletime_sec)}")
                                with col_metrics[2]:
                                    st.metric("Other Time", f"{format_seconds_to_hhmm(other_time_sec)}")
                                
                                if sisa_waktu_shift_sec > 0.1 or unaccounted_time_sec > 0.1:
                                    col_remaining = st.columns(2)
                                    if sisa_waktu_shift_sec > 0.1:
                                        with col_remaining[0]:
                                            st.metric("Time Remaining", f"{format_seconds_to_hhmm(sisa_waktu_shift_sec)}", delta_color="off")
                                    if unaccounted_time_sec > 0.1:
                                        with col_remaining[1]:
                                            st.metric("Unaccounted Time", f"{format_seconds_to_hhmm(unaccounted_time_sec)}", delta_color="off")

                            else:
                                st.info(
                                    "Tidak ada data metrik yang signifikan untuk shift ini (semua kategori nol atau terlalu kecil)."
                                )
                                logger.info(f"[ShiftMetricsPage] No significant metrics for {machine_name}-{current_shift_name}-{current_date_today}.")
                        else:
                            st.info(
                                f"Tidak ada data metrik yang ditemukan di database untuk {machine_name} pada Shift {current_shift_name.replace('_', ' ').title()} hari ini."
                            )
                            logger.info(f"[ShiftMetricsPage] No data loaded from DB for {machine_name}-{current_shift_name}-{current_date_today}.")
                    else:
                        st.info("Shift boundaries tidak ditemukan atau tidak valid untuk waktu saat ini.")
                        logger.warning(f"[ShiftMetricsPage] Shift boundaries not found or invalid for {current_date_today}-{current_shift_name}.")

# Automatic page refresh every 60 seconds
time.sleep(60)
st.rerun()