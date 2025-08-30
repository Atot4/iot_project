# app_core/opc_client_module.py

import time
import logging  # Import modul logging
from opcua import Client, ua

# Konfigurasi logging (pastikan ini konsisten dengan main_app.py jika Anda ingin log terpusat)
logger = logging.getLogger(__name__)


class OpcUaClient:
    """
    Kelas untuk mengelola koneksi dan pembacaan data dari server OPC UA.
    """

    def __init__(self, url, user, password, variables, machine_name="Unknown Machine"):
        """
        Inisialisasi klien OPC UA.

        Args:
            url (str): URL server OPC UA (misal: "opc.tcp://192.168.0.2:4840/").
            user (str): Nama pengguna untuk otentikasi.
            password (str): Kata sandi untuk otentikasi.
            variables (dict): Kamus variabel dengan nama sebagai kunci
                              dan NodeId sebagai nilai (misal: {"Status": "ns=1;s=/1000/STATUS"}).
            machine_name (str): Nama deskriptif untuk mesin ini (opsional).
        """
        self.url = url
        self.user = user
        self.password = password
        self.variables = variables
        self.machine_name = machine_name
        self.client = None
        self.connected = False

    def connect(self):
        """
        Membangun koneksi ke server OPC UA. Mengembalikan True jika berhasil, False jika gagal.
        """
        try:
            logger.info(f"[{self.machine_name}] Attempting to connect to {self.url}...")
            self.client = Client(self.url)
            self.client.set_user(self.user)
            self.client.set_password(self.password)
            self.client.connect()
            self.connected = True
            logger.info(f"[{self.machine_name}] Connected to OPC UA server.")
        except ConnectionRefusedError:
            logger.error(
                f"[{self.machine_name}] Failed to connect: Connection refused at {self.url}."
            )
            self.connected = False
        except Exception as e:
            logger.error(
                f"[{self.machine_name}] An unexpected error occurred during connection: {e}"
            )
            self.connected = False
        return self.connected

    # def disconnect(self):
    #     """
    #     Memutuskan koneksi dari server OPC UA.
    #     """
    #     if self.connected and self.client:
    #         try:
    #             self.client.disconnect()
    #             self.connected = False
    #             logger.info(f"[{self.machine_name}] Disconnected from OPC UA server.")
    #         except Exception as e:
    #             logger.error(f"[{self.machine_name}] Error during disconnection: {e}")

    def disconnect(self):
        """
        Disconnects the OPC UA client from the server.
        Includes error handling to prevent "socket not found" errors.
        """
        if self.connected:
            try:
                self.client.disconnect()
                self.connected = False
                logging.getLogger(__name__).info(f"[{self.machine_name}] Disconnected from OPC UA server.")
            except Exception as e:
                # Catching a broad exception here to handle WinError 10038 gracefully
                logging.getLogger(__name__).error(f"[{self.machine_name}] Error during disconnection: {e}")
        else:
            logging.getLogger(__name__).info(f"[{self.machine_name}] Client is already disconnected.")

    # def read_all_variables(self):
    #     """
    #     Membaca nilai dari semua variabel yang dikonfigurasi.

    #     Returns:
    #         dict: Kamus yang berisi nama variabel dan nilai-nilainya,
    #               atau None jika tidak terhubung atau ada kesalahan.
    #     """
    #     if not self.connected:
    #         return None  # Tidak perlu mencetak pesan di sini, karena run_polling sudah memeriksa koneksi

    #     read_values = {}
    #     for name, node_id in self.variables.items():
    #         try:
    #             node = self.client.get_node(node_id)
    #             value = node.get_value()
    #             read_values[name] = value
    #         except ua.UaError as e:
    #             # Kesalahan OPC UA (NodeId tidak ditemukan, Bad_NodeIdUnknown, dll.)
    #             logger.warning(
    #                 f"[{self.machine_name}] OPC UA Error reading '{name}' ({node_id}): {e}"
    #             )
    #             read_values[name] = None
    #         except Exception as e:
    #             # Kesalahan umum lainnya
    #             logger.error(
    #                 f"[{self.machine_name}] Unexpected error reading '{name}' ({node_id}): {e}"
    #             )
    #             read_values[name] = None
    #     return read_values

    def read_all_variables(self):
        """
        Membaca nilai dari semua variabel yang dikonfigurasi.
        
        Returns:
            dict: Kamus yang berisi nama variabel dan nilai-nilainya,
                atau None jika tidak terhubung, ada kesalahan, atau tidak ada variabel yang berhasil dibaca.
        """
        if not self.connected:
            logger.warning(f"[{self.machine_name}] Not connected. Cannot read variables.")
            return None

        read_values = {}
        for name, node_id in self.variables.items():
            try:
                node = self.client.get_node(node_id)
                value = node.get_value()
                read_values[name] = value
            except ua.UaError as e:
                logger.warning(
                    f"[{self.machine_name}] OPC UA Error reading '{name}' ({node_id}): {e}"
                )
                # read_values[name] = None
            except Exception as e:
                logger.error(
                    f"[{self.machine_name}] Unexpected error reading '{name}' ({node_id}): {e}"
                )
                # read_values[name] = None

        # Tambahkan pemeriksaan ini di akhir fungsi
        if not read_values:
            logger.warning(f"[{self.machine_name}] No variables were successfully read. Returning None.")
            return None

        return read_values
