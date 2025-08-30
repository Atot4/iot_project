import pandas as pd
import datetime

# Fungsi untuk mengonversi string waktu 'HH:MM:SS' menjadi total detik
def convert_time_to_seconds(time_str):
    """
    Mengonversi string waktu dalam format HH:MM:SS menjadi total detik.
    """
    try:
        # Menghapus spasi di awal atau akhir string
        time_str = time_str.strip()
        if len(time_str.split(':')) == 2:
            time_str = f"0:{time_str}"
        h, m, s = map(int, time_str.split(':'))
        return datetime.timedelta(hours=h, minutes=m, seconds=s).total_seconds()
    except (ValueError, AttributeError):
        return None
    
def clean_program_name(name):
    """
    Membersihkan nama program dengan menghapus ekstensi yang umum.
    """
    if not isinstance(name, str):
        return name
    
    # Daftar ekstensi umum dalam huruf kecil
    possible_extensions = ['.nc', '.h']
    
    name_lower = name.lower()
    for ext in possible_extensions:
        if name_lower.endswith(ext):
            return name[:-len(ext)]
    return name


# Fungsi untuk memproses DataFrame mentah dari CSV
def process_raw_csv_data(df_raw, file_name):
    """
    Menerapkan logika konversi dari konversi.py pada DataFrame yang sudah dimuat.
    """
    # Mengubah nama kolom 'Cycle' menjadi 'Notes'
    df_raw.rename(columns={'Cycle': 'Notes'}, inplace=True)
    
    # Menambah kolom 'program_name'
    df_raw['program_name'] = file_name + df_raw['Job #'].astype(str)

    df_raw['program_name'] = df_raw['program_name'].apply(clean_program_name)

    # Konversi kolom 'Machining time' menjadi total detik
    df_raw['Machining_time_seconds'] = df_raw['Machining time'].apply(convert_time_to_seconds)

    # Menambahkan kolom 'Target Durasi (menit)'
    df_raw['target_duration (min)'] = (df_raw['Machining_time_seconds'] / 60).round(2)

    # Tambahkan Quantity
    df_raw['Quantity'] = 1
    
    # Menambahkan kolom 'Prog. Feedrate' dengan logika kondisional
    df_raw['Notes'] = df_raw['Notes'].fillna('')
    df_raw['target_feedrate'] = df_raw.apply(
        lambda row: row['Z feedrate'] if any(word in str(row['Notes']).lower() for word in ['centering', 'drill', 'tap'])
        else row['Plane feedrate'],
        axis=1
    )
    
    # Tentukan kolom yang akan dikembalikan, sekarang termasuk 'Remarks'
    columns_to_keep = ['program_name', 'target_duration (min)', 'Spindle RPM', 'target_feedrate', 'Quantity', 'Notes', 'Remarks']
    
    # Periksa apakah kolom 'Remarks' ada, jika tidak, tambahkan dengan nilai kosong
    if 'Remarks' not in df_raw.columns:
        df_raw['Remarks'] = ""
    
    return df_raw[columns_to_keep].copy()