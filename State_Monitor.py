# State_Monitor.py

import streamlit as st
import json
import time
import os
import pandas as pd
import plotly.express as px  # Meskipun tidak digunakan untuk grafik di sini, biarkan saja jika suatu saat perlu
import datetime
import sys
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Pastikan path ke config sudah benar agar dapat diakses dari halaman
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app_core.config import (
    DATA_FILE,
    RUNNING_STATUSES,
    IDLE_STATUSES,
    MACHINE_DISPLAY_ORDER,
)

# --- Helper Functions ---
def load_json_data(filepath):
    """
    Memuat data dari file JSON.
    Menangani kasus file tidak ada, format JSON tidak valid, atau file kosong.
    """
    if not os.path.exists(filepath):
        # logging.warning(f"File data '{filepath}' tidak ditemukan.")
        return None
    
    # Tambahkan pemeriksaan untuk file kosong
    if os.path.getsize(filepath) == 0:
        logging.warning(f"File data '{filepath}' kosong. Mengabaikan pembacaan.")
        return None
        
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {filepath}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading {filepath}: {e}")
        return None


# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Machine Monitor")  # Ubah judul halaman
st.title("State Monitor")  # Ubah judul utama dashboard

# --- Main App Logic for Live Data ---
# Placeholder untuk pesan status (misalnya, "Menunggu data...")
status_message = st.empty()

# Muat data real-time terbaru dari machine_data.json
machine_data = load_json_data(DATA_FILE)

# Tampilkan pesan jika data mesin belum tersedia
if not machine_data:
    print(
        "Menunggu data mesin... Pastikan program utama (`main_app.py`) sedang berjalan dan menghasilkan file `machine_data.json`."
    )
else:
    status_message.empty()  # Hapus pesan jika data sudah ada

    # Dapatkan nama mesin yang aktif dari data terbaru
    active_machine_names = list(machine_data.keys()) if machine_data else []
    
    # Buat daftar mesin yang akan ditampilkan sesuai urutan yang didefinisikan
    ordered_machine_names = []
    for machine_name in MACHINE_DISPLAY_ORDER:
        if machine_name in active_machine_names:
            ordered_machine_names.append(machine_name)

    # Tambahkan mesin yang tidak ada di MACHINE_DISPLAY_ORDER (jika ada) ke bagian akhir, diurutkan alfabetis
    remaining_machines = sorted(
        [name for name in active_machine_names if name not in MACHINE_DISPLAY_ORDER]
    )
    ordered_machine_names.extend(remaining_machines)

    if not ordered_machine_names:
        st.info("Tidak ada data mesin yang tersedia saat ini.")
    else:
        # Tampilkan timestamp terakhir data yang diproses
        last_updated_time = "N/A"
        for machine_name, data in machine_data.items():
            if "Timestamp_Processed" in data:
                last_updated_time = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(data["Timestamp_Processed"])
                )
                break
        st.info(f"Last Updated: {last_updated_time}")

        # Jumlah kolom untuk tampilan mesin
        cols_per_row = 4 # Kembali ke 4 kolom karena tidak ada grafik besar di sini
        num_machines = len(ordered_machine_names)

        for i in range(0, num_machines, cols_per_row):
            cols = st.columns(cols_per_row)
            for j, machine_name in enumerate(
                ordered_machine_names[i : i + cols_per_row]
            ):
                if machine_name in machine_data:
                    with cols[j]:
                        #st.subheader(f"{machine_name}")

                        machine_info = machine_data[machine_name]
                        status_text = machine_info.get("Status_Text", "N/A")
                        spindle_speed = machine_info.get("Spindle_Speed", "N/A")
                        feedrate = machine_info.get("FeedRate_mm_per_min", "N/A")
                        current_program = machine_info.get("Current_Program", "N/A")
                        moden = machine_info.get("Moden", "N/A")
                        motion = machine_info.get("Motion", "N/A")
                        ovrspindle = machine_info.get("OvrSpindle", "N/A")
                        ovrfeed = machine_info.get("OvrFeed", "N/A")
                        
                        with st.container(border=False):
                            # Menentukan warna status
                            status_color = 'grey' # Default
                            if status_text in RUNNING_STATUSES:
                                status_color = 'green'
                            elif status_text in IDLE_STATUSES:
                                status_color = 'orange'
                            else: # Untuk status lainnya seperti "Disconnected", "Alarm", "Undefined Status"
                                status_color = 'red'

                            # st.markdown(
                            #     f"**Status:** <span style='color: {status_color}; font-weight: bold;'>{status_text}</span>",
                            #     unsafe_allow_html=True,
                            # )
                            st.markdown(f"""
                                <div style='border-radius: 5px; padding: 10px;'>
                                    <div style='display: flex; justify-content: space-between; align-items: center;'>
                                        <span style='font-size: 20px; font-weight: bold;'>{machine_name}</span>
                                        <span style='color: {status_color}; font-weight: bold;'>{status_text}</span>
                                    </div>
                                    <hr style='border: 1px solid #ccc; padding: 0px; margin: 0px'>
                                    <div>
                                        <span style='font-weight: bold;'>Program:</span> {current_program}<br>
                                        <span style='font-weight: bold;'>Spindle:</span> {spindle_speed}<br>
                                        <span style='font-weight: bold;'>Feedrate:</span> {feedrate}
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)
                            # st.write(f"**Program:** {current_program}")
                            # st.write(f"**Spindle:** {spindle_speed} RPM")
                            # st.write(f"**Feedrate:** {feedrate} mm/min")
                            # st.write(f"**Ovr Spindle:** {ovrspindle} %")
                            # st.write(f"**Ovr Feed:** {ovrfeed} %")

                else:
                    with cols[j]:
                        st.empty()  # Placeholder untuk kolom kosong

# Otomatis refresh halaman setiap 5 detik
time.sleep(1)
st.rerun()
