# app_core/data_processor.py

import logging
import time
from opcua import ua # Pastikan ua dari opcua diimpor

logger = logging.getLogger(__name__)

# --- machine state definitions ---
HEIDENHAIN_STATUS_MAP = {
    0: "Disconnected", 1: "Connected but not sending data", 2: "Running",
    3: "Manual mode", 4: "Interrupted", 5: "Waiting", None: "N/A",
}

FANUC_YASDA_STATUS_MAP = {
    0: "Disconnected", 1: "Connected but not sending data", 2: "Running",
    3: "Manual mode", 4: "Interrupted", 5: "Waiting", None: "N/A",
}

MITSUBISHI_WELE_STATUS_MAP = {
    0: "Disconnected", 1: "Connected but not sending data", 2: "Running",
    3: "Manual mode", 4: "Interrupted", 5: "Waiting", None: "N/A",
}

MITSUBISHI_QUASER_STATUS_MAP = {
    0: "NC Reset", 1: "Emergency", 2: "Ready", 3: "Running",
    4: "With Synchronization", 5: "Waiting", 6: "Stop", 7: "Hold", None: "N/A",
}

MAKINO_MODEN_MOTION_STATUS_MAP = {
    (10, 1): "Running", (10, 0): "Ready", (0, None): "MDI", (1, None): "Memory",
    (2, None): "****", (3, None): "Edit", (4, None): "Handle", (5, None): "JOG",
    (6, None): "Teach in JOG", (7, None): "Teach in Handle", (8, None): "INC·feed",
    (9, None): "Reference", (11, None): "TEST", (None, None): "N/A",
}

DEFAULT_STATUS_MAP = {
    0: "Disconnected", 1: "Connected but not sending data", 2: "Running",
    3: "Manual mode", 4: "Interrupted", 5: "Faulted", None: "N/A",
}

# --- Helper function to safely get value from OPC UA Variant ---
def _get_opcua_value(variant_or_value):
    if isinstance(variant_or_value, ua.Variant):
        return variant_or_value.value
    return variant_or_value

def process_opcua_data(machine_name, raw_data):
    processed_output = {}
    
    machine_name_lower = machine_name.lower()

    # --- Status Mapping Logic ---
    current_status_map = DEFAULT_STATUS_MAP
    status_key_to_use = None
    status_text_derived = "Undefined Status"

    if "makino" in machine_name_lower:
        moden_raw = _get_opcua_value(raw_data.get("Moden"))
        motion_raw = _get_opcua_value(raw_data.get("Motion"))
        
        moden_int = None
        motion_int = None

        try:
            if moden_raw is not None:
                moden_int = int(float(moden_raw)) # Convert safely
        except (ValueError, TypeError):
            logger.warning(f"[{machine_name}] Could not convert Moden '{moden_raw}' to integer. Using None.")
        
        try:
            if motion_raw is not None:
                motion_int = int(float(motion_raw)) # Convert safely
        except (ValueError, TypeError):
            logger.warning(f"[{machine_name}] Could not convert Motion '{motion_raw}' to integer. Using None.")

        status_text_derived = MAKINO_MODEN_MOTION_STATUS_MAP.get((moden_int, motion_int), "Undefined Status")
        if status_text_derived == "Undefined Status" and moden_int is not None:
              status_text_derived = MAKINO_MODEN_MOTION_STATUS_MAP.get((moden_int, None), "Undefined Status")


    elif "yasda" in machine_name_lower:
        current_status_map = FANUC_YASDA_STATUS_MAP
        status_key_to_use = "Status"
    elif "wele" in machine_name_lower:
        current_status_map = MITSUBISHI_WELE_STATUS_MAP
        status_key_to_use = "Status"
    elif "quaser" in machine_name_lower:
        current_status_map = MITSUBISHI_QUASER_STATUS_MAP
        status_key_to_use = "State_Number"
    elif (
        "hpm" in machine_name_lower
        or "hsm" in machine_name_lower
        or "p500" in machine_name_lower
    ):
        current_status_map = HEIDENHAIN_STATUS_MAP
        status_key_to_use = "State_Number"
    else: # Fallback for unknown machines
        if "Status" in raw_data:
            status_key_to_use = "Status"
        elif "State_Number" in raw_data:
            status_key_to_use = "State_Number"
        else:
            logger.warning(f"[{machine_name}] Neither 'Status' nor 'State_Number' variable found in raw data. Using default map.")
            status_key_to_use = "N/A_Fallback"

    # Process status if not Makino-specific
    if status_key_to_use and status_key_to_use != "Moden_Motion" and status_key_to_use != "N/A_Fallback":
        current_status_raw = _get_opcua_value(raw_data.get(status_key_to_use))
        current_status_int = None
        if current_status_raw is not None:
            try:
                current_status_int = int(float(current_status_raw)) # Safely convert to int
            except (ValueError, TypeError):
                logger.warning(f"[{machine_name}] Could not convert status '{current_status_raw}' to integer. Using raw value.")
        status_text_derived = current_status_map.get(current_status_int, "Undefined Status")
    elif status_key_to_use == "N/A_Fallback":
        status_text_derived = DEFAULT_STATUS_MAP.get(None, "Undefined Status")

    processed_output["Raw_Status_Key_Used"] = status_key_to_use
    if status_key_to_use == "Moden_Motion":
        processed_output["Raw_Status_Value"] = f"Moden:{moden_raw}, Motion:{motion_raw}"
    elif status_key_to_use == "N/A_Fallback":
        processed_output["Raw_Status_Value"] = None
    else:
        processed_output["Raw_Status_Value"] = _get_opcua_value(raw_data.get(status_key_to_use))

    processed_output["Status_Text"] = status_text_derived

    # --- Process FeedRate ---
    feed_rate = _get_opcua_value(raw_data.get("FeedRate"))
    if feed_rate is not None:
        try:
            processed_output["FeedRate_mm_per_min"] = int(float(feed_rate))
        except (ValueError, TypeError):
            logger.warning(f"[{machine_name}] Could not convert FeedRate '{feed_rate}' to integer.")
            processed_output["FeedRate_mm_per_min"] = None
    else:
        processed_output["FeedRate_mm_per_min"] = None

    # --- Process Spindle Speed ---
    spindle_speed = _get_opcua_value(raw_data.get("Spindle"))
    if spindle_speed is not None:
        try:
            processed_output["Spindle_Speed"] = int(float(spindle_speed))
        except (ValueError, TypeError):
            logger.warning(f"[{machine_name}] Could not convert Spindle Speed '{spindle_speed}' to integer.")
            processed_output["Spindle_Speed"] = None
    else:
        processed_output["Spindle_Speed"] = None

    # --- PERBAIKAN PENTING: Pemrosesan Current_Program untuk Makino dan non-Makino yang lebih robust ---
    current_program_value = None # Inisialisasi di awal untuk memastikan selalu memiliki nilai default
    
    if "v77" in machine_name_lower or "f5" in machine_name_lower or "v33" in machine_name_lower:
        program_num = _get_opcua_value(raw_data.get("Program_num"))
        setting_num = _get_opcua_value(raw_data.get("Setting_num"))
        sub_process_num = _get_opcua_value(raw_data.get("Sub_process_num"))
        program_id = _get_opcua_value(raw_data.get("Program_id"))

        program_parts_for_makino = []

        # Periksa dan konversi Program_num dengan aman
        if program_num is not None:
            try:
                program_num_int = int(float(program_num))
                if program_num_int is not None and str(program_num_int).strip() != "" and program_num_int != 0:
                    program_parts_for_makino.append(f"N{str(program_num_int).strip()}-")
            except (ValueError, TypeError):
                logger.warning(f"[{machine_name}] Could not convert Program_num '{program_num}' to integer. Skipping.")
        
        # Periksa dan konversi Setting_num dengan aman
        setting_sub_str = ""
        if setting_num is not None:
            try:
                setting_num_int = int(float(setting_num))
                if setting_num_int is not None and str(setting_num_int).strip() != "":
                    setting_sub_str += str(setting_num_int).strip()
            except (ValueError, TypeError):
                logger.warning(f"[{machine_name}] Could not convert Setting_num '{setting_num}' to integer. Skipping.")

        # Periksa dan konversi Sub_process_num dengan aman
        if sub_process_num is not None:
            try:
                sub_process_int = int(float(sub_process_num))
                if 1 <= sub_process_int <= 26: 
                    setting_sub_str += chr(sub_process_int + 64)
                elif sub_process_int == 0:
                    pass 
                else: 
                    logger.warning(f"[{machine_name}] Sub_process_num '{sub_process_num}' (converted to {sub_process_int}) out of expected range (1-26 for A-Z). Skipping char conversion.")
            except (ValueError, TypeError):
                logger.warning(f"[{machine_name}] Could not convert Sub_process_num '{sub_process_num}' to a valid number for char conversion. Skipping.")
        
        if setting_sub_str:
            program_parts_for_makino.append(setting_sub_str)

        # Periksa dan konversi Program_id dengan aman
        if program_id is not None:
            try:
                program_id_int = int(float(program_id))
                if program_id_int is not None and str(program_id_int).strip() != "":
                    program_parts_for_makino.append(str(program_id_int).strip())
            except (ValueError, TypeError):
                logger.warning(f"[{machine_name}] Could not convert Program_id '{program_id}' to integer. Skipping.")

        if program_parts_for_makino:
            # Menggabungkan parts.
            current_program_value = "".join(program_parts_for_makino)
            if current_program_value.endswith("-"): # Menghapus tanda hubung jika tidak ada kode di belakangnya
                current_program_value = current_program_value[:-1]
            if current_program_value == "":
                 current_program_value = None
    else: # Untuk mesin non-Makino, coba beberapa NodeId umum
        program_node_ids = ["Program", "Current_Program", "ProgramName", "PathProgramName", "ActiveProgramName", "PROGN"]
        found_program_name = None
        for node_id_key in program_node_ids:
            program_val = _get_opcua_value(raw_data.get(node_id_key))
            if program_val is not None and str(program_val).strip() != "":
                found_program_name = str(program_val).strip()
                break
        current_program_value = found_program_name
    
    # Simpan Current_Program ke processed_output. Ini hanya boleh ada SATU KALI
    processed_output["Current_Program"] = current_program_value

    # --- AKHIR PERBAIKAN PENTING ---

    # --- Process other raw data directly or with type conversion ---
    # These sections remain largely unchanged, but ensure they use _get_opcua_value
    processed_output["Moden"] = _get_opcua_value(raw_data.get("Moden"))
    processed_output["Motion"] = _get_opcua_value(raw_data.get("Motion"))
    processed_output["State_Number"] = _get_opcua_value(raw_data.get("State_Number"))
    processed_output["OvrSpindle"] = _get_opcua_value(raw_data.get("OvrSpindle"))
    processed_output["OvrFeed"] = _get_opcua_value(raw_data.get("OvrFeed"))
    processed_output["Status"] = _get_opcua_value(raw_data.get("Status"))

    processed_output["Timestamp_Processed"] = time.time()

    return processed_output

def get_mode(series):
            if series.empty or series.isnull().all():
                return 0
            non_zero_series = series[series > 0]
            if not non_zero_series.empty:
                return non_zero_series.mode().iloc[0]
            else:
                return series.mode().iloc[0] if not series.empty else 0
            
