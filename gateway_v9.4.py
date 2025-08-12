#!/usr/bin/env python3
# gateway_v9.4.py - Startup Optimization
# Deskripsi: Gateway IEC 61850 ke IEC 60870-5-104.
# Fitur v9.4: Mengoptimalkan startup dengan hanya membuat index.html jika
#             file tersebut belum ada, dan mengandalkan cache model IED
#             untuk mempercepat koneksi setelah eksekusi pertama.

import asyncio
import json
import logging
import configparser
import threading
import sys
import os
import time
from urllib.parse import urlparse
import http.server
import socketserver
import websockets

try:
    import libiec61850client_cached as libiec61850client
    import libiec60870server
    from lib60870 import *
    from lib61850 import IedConnection_getState
except ImportError as e:
    print(f"Error: Gagal mengimpor library yang dibutuhkan. Pesan: {e}")
    sys.exit(1)

# --- Definisikan konstanta ---
CON_STATE_NOT_CONNECTED, CON_STATE_CONNECTING, CON_STATE_CONNECTED, CON_STATE_CLOSING, CON_STATE_CLOSED = 0, 1, 2, 3, 4
FALLBACK_POLLING_INTERVAL = 10
HEARTBEAT_POLLING_INTERVAL = 60
RECONNECT_DELAY = 15
HTTP_PORT = 8000
WEBSOCKET_PORT = 8001

# --- Variabel & Objek Global ---
clients_dict_lock = threading.Lock()
ied_locks = {}
ied_clients = {}
ied_to_ioas_map, mms_to_ioa_map, ioa_inversion_map, ioa_to_mms_config, mms_to_value_path_map, ioa_to_full_address_map = {}, {}, {}, {}, {}, {}
processing_queue = None
broadcast_queue = None
shutdown_event = None
iec104_server = None
main_loop = None
websocket_clients = set()
realtime_data_cache = {}
cache_lock = asyncio.Lock()

# --- Fungsi-fungsi untuk Server Web ---
def start_http_server():
    class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
        def end_headers(self):
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            super().end_headers()
    
    web_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(web_dir)
    with socketserver.TCPServer(("", HTTP_PORT), NoCacheHandler) as httpd:
        logging.info(f"HTTP server menyajikan file dari '{web_dir}' di port {HTTP_PORT}")
        httpd.serve_forever()

async def websocket_handler(websocket, path):
    logging.info(f"WebSocket client terhubung: {websocket.remote_address}")
    async with cache_lock:
        if realtime_data_cache:
            logging.info(f"Mengirim {len(realtime_data_cache)} item dari cache ke client baru.")
            tasks = [websocket.send(json.dumps(data)) for data in realtime_data_cache.values()]
            await asyncio.gather(*tasks, return_exceptions=True)
    websocket_clients.add(websocket)
    try:
        await websocket.wait_closed()
    except websockets.exceptions.ConnectionClosed:
        logging.info(f"Koneksi WebSocket ditutup secara normal dari sisi client: {websocket.remote_address}")
    finally:
        logging.info(f"WebSocket client terputus: {websocket.remote_address}")
        websocket_clients.remove(websocket)

async def broadcast_updates_task(queue):
    logging.info("Broadcast task dimulai.")
    while not (shutdown_event and shutdown_event.is_set()):
        try:
            update = await queue.get()
            async with cache_lock:
                realtime_data_cache[update['ioa']] = update
            if websocket_clients:
                message = json.dumps(update)
                clients_to_send = list(websocket_clients)
                tasks = [client.send(message) for client in clients_to_send]
                await asyncio.gather(*tasks, return_exceptions=True)
            queue.task_done()
        except asyncio.CancelledError:
            logging.info("Broadcast task dihentikan.")
            break
        except Exception as e:
            logging.error(f"Error di dalam broadcast_updates_task: {e}", exc_info=True)

# --- Fungsi-fungsi utilitas & callback ---
def find_first_float(data):
    if isinstance(data, (float, int)): return float(data)
    if isinstance(data, dict):
        for val in data.values():
            result = find_first_float(val)
            if result is not None: return result
    if isinstance(data, list):
        for item in data:
            result = find_first_float(item)
            if result is not None: return result
    return None

def get_value_by_path(data_dict, path_str):
    keys = path_str.split('.')
    current_level = data_dict
    try:
        for key in keys: current_level = current_level[key]
        if isinstance(current_level, (int, float)): return float(current_level)
        return None
    except (KeyError, TypeError): return None

def command_60870_callback(ioa, ioa_data, srv, select_value):
    config_line = ioa_to_mms_config.get(ioa)
    if not config_line: return -1
    uri_part = config_line.split('#')[0]
    try:
        parsed_uri = urlparse(uri_part)
        ied_id = f"{parsed_uri.hostname}:{parsed_uri.port or 102}"
    except Exception: return -1
    with clients_dict_lock: client = ied_clients.get(ied_id)
    if not client:
        logging.error(f"Perintah untuk {ied_id} gagal: client tidak terhubung.")
        return -1
    ied_lock = ied_locks.get(ied_id)
    if not ied_lock:
        logging.error(f"Perintah untuk {ied_id} gagal: lock tidak ditemukan.")
        return -1
    with ied_lock:
        val_str = "true" if ioa_data['data'] == 1 else "false"
        if select_value: return client.select(str(uri_part), val_str)
        else: return client.operate(str(uri_part), val_str)

def process_data_update(ied_id, key, data):
    if not isinstance(data, dict) or 'value' not in data: return
    reported_key, value_to_update = key, data['value']
    mms_path_from_key = reported_key
    if "iec61850://" in reported_key:
        try:
            parsed_uri = urlparse(reported_key)
            mms_path_from_key = parsed_uri.path.lstrip('/')
        except Exception:
            logging.warning(f"Tidak dapat mem-parsing URI key: {reported_key}")
            return
    valid_ioas_for_ied = set(ied_to_ioas_map.get(ied_id, []))
    if not valid_ioas_for_ied: return
    for config_path, ioa in mms_to_ioa_map.items():
        if ioa in valid_ioas_for_ied and config_path.startswith(mms_path_from_key):
            value_path = mms_to_value_path_map.get(config_path)
            final_value = get_value_by_path(value_to_update, value_path) if value_path else find_first_float(value_to_update)
            if final_value is None:
                logging.debug(f"[{ied_id}] Tidak dapat mengekstrak nilai numerik untuk key {reported_key} (IOA {ioa}).")
                continue
            try:
                ioa_type_class = iec104_server.IOA_list.get(ioa, {}).get('type')
                ioa_type = str(ioa_type_class)
                value_to_send = float(final_value)
                if "DoublePointInformation" in ioa_type: value_to_send = {1.0: 1, 2.0: 2}.get(value_to_send, 0)
                elif "SinglePointInformation" in ioa_type: value_to_send = 1 if int(value_to_send) != 0 else 0
                if ioa_inversion_map.get(ioa, False): value_to_send = {1: 2, 2: 1}.get(value_to_send, value_to_send)
                update_payload = {
                    'type': 'data_update',
                    'ioa': ioa,
                    'value': value_to_send,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'address': ioa_to_full_address_map.get(ioa, "N/A")
                }
                if main_loop and main_loop.is_running():
                    main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, update_payload)
                iec104_server.update_ioa(ioa, value_to_send)
                logging.info(f"[{ied_id}] Cocok '{reported_key}' -> IOA {ioa}, diperbarui dengan: {value_to_send}")
            except Exception as e:
                logging.error(f"Error saat memproses update untuk IOA {ioa}: {e}", exc_info=True)
            return
    logging.debug(f"[{ied_id}] Tidak ada konfigurasi yang cocok untuk key: {reported_key}")

def do_invalidation(ied_id):
    if ied_id in ied_to_ioas_map:
        ioas_to_invalidate = ied_to_ioas_map[ied_id]
        logging.warning(f"Membuat invalid {len(ioas_to_invalidate)} titik data untuk {ied_id}.")
        al_params = iec104_server.alParams
        quality_flags = 48
        for ioa in ioas_to_invalidate:
            update_payload = {
                'type': 'invalidation',
                'ioa': ioa,
                'value': 'INVALID',
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'address': ioa_to_full_address_map.get(ioa, "N/A")
            }
            if main_loop and main_loop.is_running():
                main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, update_payload)
            ioa_config = iec104_server.IOA_list.get(ioa)
            if ioa_config:
                io_type = ioa_config.get('type')
                creator_func = {
                    MeasuredValueScaled: MeasuredValueScaled_create,
                    MeasuredValueShort: MeasuredValueShort_create,
                    SinglePointInformation: SinglePointInformation_create,
                    DoublePointInformation: DoublePointInformation_create
                }.get(io_type)
                if creator_func:
                    new_asdu = CS101_ASDU_create(al_params, False, CS101_COT_SPONTANEOUS, 0, 1, False, False)
                    io = cast(creator_func(None, ioa, 0, quality_flags), InformationObject)
                    CS101_ASDU_addInformationObject(new_asdu, io)
                    InformationObject_destroy(io)
                    CS104_Slave_enqueueASDU(iec104_server.slave, new_asdu)
                    CS101_ASDU_destroy(new_asdu)
        for ioa in ioas_to_invalidate:
            if ioa in iec104_server.IOA_list:
                iec104_server.IOA_list[ioa]['data'] = float('nan')

def ied_data_callback(key, data, ied_id):
    if main_loop and processing_queue and main_loop.is_running():
        update_item = {'type': 'process_data', 'ied_id': ied_id, 'key': key, 'data': data}
        main_loop.call_soon_threadsafe(processing_queue.put_nowait, update_item)
    else:
        logging.warning(f"[{ied_id}] Main loop/queue tidak tersedia, titik data diabaikan.")

def invalidate_ied_points(ied_id):
    if main_loop and processing_queue and main_loop.is_running():
        update_item = {'type': 'invalidate', 'ied_id': ied_id}
        main_loop.call_soon_threadsafe(processing_queue.put_nowait, update_item)
    else:
        logging.warning(f"[{ied_id}] Main loop/queue tidak tersedia, invalidasi diabaikan.")

# --- ASYNC TASKS ---
async def data_processor_task(queue):
    logging.info("Data processor task dimulai.")
    loop = asyncio.get_running_loop()
    while not (shutdown_event and shutdown_event.is_set()):
        try:
            item = await queue.get()
            if item.get('type') == 'process_data':
                await loop.run_in_executor(None, process_data_update, item['ied_id'], item['key'], item['data'])
            elif item.get('type') == 'invalidate':
                await loop.run_in_executor(None, do_invalidation, item['ied_id'])
            queue.task_done()
        except asyncio.CancelledError:
            logging.info("Data processor task dihentikan.")
            break
        except Exception as e:
            logging.error(f"Error di dalam data_processor_task: {e}", exc_info=True)

async def ied_handler(ied_id, uris):
    logging.info(f"[{ied_id}] IED handler task dimulai.")
    loop = asyncio.get_running_loop()
    client = None
    active_polling_interval = FALLBACK_POLLING_INTERVAL
    ied_locks[ied_id] = threading.Lock()
    def polling_entry_point(key, data):
        logging.debug(f"[{ied_id}] Data diterima via POLLING untuk key: {key}")
        ied_data_callback(key, data, ied_id)
    def report_entry_point(key, data):
        logging.debug(f"[{ied_id}] Data diterima via REPORT untuk key: {key}")
        ied_data_callback(key, data, ied_id)
    def locked_check_state():
        with ied_locks[ied_id]:
            if not client or not client.getRegisteredIEDs().get(ied_id, {}).get('con'): return False
            conn_info = client.getRegisteredIEDs()[ied_id]
            if conn_info.get('con'): return IedConnection_getState(conn_info['con']) == CON_STATE_CONNECTED
            return False
    def locked_register_values():
        with ied_locks[ied_id]:
            for uri in uris: client.registerReadValue(str(uri))
            return len(client.polling)
    while not (shutdown_event and shutdown_event.is_set()):
        try:
            logging.info(f"[{ied_id}] Mencoba menghubungkan...")
            with ied_locks[ied_id]:
                client = libiec61850client.iec61850client(readvaluecallback=polling_entry_point, loggerRef=logging, cmdTerm_cb=None, Rpt_cb=report_entry_point)
            res = await loop.run_in_executor(None, client.getIED, ied_id.split(':')[0], int(ied_id.split(':')[1]))
            if res != 0: raise ConnectionError("getIED gagal, error koneksi atau discovery.")
            with clients_dict_lock: ied_clients[ied_id] = client
            logging.info(f"[{ied_id}] Koneksi berhasil. Mendaftarkan nilai...")
            polling_item_count = await loop.run_in_executor(None, locked_register_values)
            active_polling_interval = HEARTBEAT_POLLING_INTERVAL if polling_item_count == 0 else FALLBACK_POLLING_INTERVAL
            mode = "Heartbeat" if active_polling_interval == HEARTBEAT_POLLING_INTERVAL else "Fallback Polling"
            logging.info(f"[{ied_id}] Mode: {mode}. Interval polling diatur ke {active_polling_interval} detik.")
            while not (shutdown_event and shutdown_event.is_set()):
                if not await loop.run_in_executor(None, locked_check_state):
                    raise ConnectionError("Koneksi terputus (cek proaktif).")
                await loop.run_in_executor(None, client.poll)
                await asyncio.sleep(active_polling_interval)
        except ConnectionError as e:
            logging.error(f"[{ied_id}] Handler error: {e}. Menghubungkan ulang dalam {RECONNECT_DELAY} detik.")
        except Exception as e:
            logging.error(f"[{ied_id}] Handler error tak terduga: {e}", exc_info=True)
        finally:
             with clients_dict_lock:
                if ied_id in ied_clients: del ied_clients[ied_id]
             invalidate_ied_points(ied_id)
             try: await asyncio.sleep(RECONNECT_DELAY)
             except asyncio.CancelledError: break

async def main():
    global iec104_server, main_loop, processing_queue, broadcast_queue, shutdown_event, mms_to_value_path_map
    
    main_loop = asyncio.get_running_loop()
    processing_queue = asyncio.Queue()
    broadcast_queue = asyncio.Queue()
    shutdown_event = asyncio.Event()

    logging.basicConfig(format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s', level=logging.INFO)
    logger = logging.getLogger('gateway-v9.4')

    config = configparser.ConfigParser(); config.optionxform = str
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'config.local.ini'
    if not os.path.exists(config_file): logger.error(f"File konfigurasi tidak ditemukan: {config_file}"); sys.exit(1)
    config.read(config_file)
    logger.info("Gateway v9.4 (Optimized Startup) dimulai")

    create_index_html_if_not_exists()
    http_thread = threading.Thread(target=start_http_server, name="HTTPServerThread", daemon=True)
    http_thread.start()

    websocket_server = await websockets.serve(websocket_handler, "0.0.0.0", WEBSOCKET_PORT)
    logger.info(f"WebSocket server dimulai di port {WEBSOCKET_PORT}")

    iec104_server = libiec60870server.IEC60870_5_104_server()
    data_types = {'measuredvaluescaled': MeasuredValueScaled, 'measuredvaluefloat': MeasuredValueShort,
                  'singlepointinformation': SinglePointInformation, 'doublepointinformation': DoublePointInformation}
    command_types = {'singlepointcommand': SingleCommand, 'doublepointcommand': DoubleCommand}

    logger.info("Mem-parsing konfigurasi...")
    ied_data_groups = {}
    all_sections = list(data_types.keys()) + list(command_types.keys())
    for section in all_sections:
        if section in config:
            for ioa, config_line in config[section].items():
                uri_part, should_invert, value_path = config_line, False, None
                if ':invers=true' in uri_part: uri_part, should_invert = uri_part.replace(':invers=true', ''), True
                if '#' in uri_part: uri_part, value_path = uri_part.split('#', 1)
                parsed = urlparse(uri_part)
                mms_path = parsed.path.lstrip('/')
                ied_id = f"{parsed.hostname}:{parsed.port or 102}"
                ioa_int = int(ioa)
                ioa_to_full_address_map[ioa_int] = config_line
                if ied_id not in ied_to_ioas_map: ied_to_ioas_map[ied_id] = []
                if ioa_int not in ied_to_ioas_map[ied_id]: ied_to_ioas_map[ied_id].append(ioa_int)
                if section in data_types:
                    mms_to_ioa_map[mms_path] = ioa_int
                    if value_path: mms_to_value_path_map[mms_path] = value_path
                    if ied_id not in ied_data_groups: ied_data_groups[ied_id] = []
                    if uri_part not in ied_data_groups[ied_id]: ied_data_groups[ied_id].append(uri_part)
                if should_invert: ioa_inversion_map[ioa_int] = True
                if section in command_types: ioa_to_mms_config[ioa_int] = config_line

    logger.info(f"Menemukan {len(ied_data_groups)} IED unik untuk dimonitor.")
    for section, mms_type in data_types.items():
        if section in config:
            for item in config[section]: iec104_server.add_ioa(int(item), mms_type, 0, None, True)
    for section, mms_type in command_types.items():
        if section in config:
            for item in config[section]: iec104_server.add_ioa(int(item), mms_type, 0, command_60870_callback, False)

    server_thread = threading.Thread(target=iec104_server.start, name="IEC104ServerThread", daemon=True)
    server_thread.start()
    logger.info("Server IEC 104 dimulai di thread terpisah.")
    
    data_task = asyncio.create_task(data_processor_task(processing_queue))
    broadcast_task = asyncio.create_task(broadcast_updates_task(broadcast_queue))
    ied_handler_tasks = [asyncio.create_task(ied_handler(ied_id, uris)) for ied_id, uris in ied_data_groups.items()]
    
    all_tasks = ied_handler_tasks + [data_task, broadcast_task]
    try:
        await asyncio.gather(*all_tasks)
    except Exception as e:
        logger.error(f"Grup task utama mengalami error: {e}")
    finally:
        logger.info("Mematikan semua task...")
        shutdown_event.set()
        for task in all_tasks: task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        websocket_server.close()
        await websocket_server.wait_closed()
        if iec104_server: iec104_server.stop()
        logger.info("Gateway berhenti.")

# PERUBAHAN: Fungsi ini sekarang hanya akan membuat file jika belum ada.
def create_index_html_if_not_exists():
    """Membuat file index.html jika belum ada di direktori yang sama dengan skrip."""
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if not os.path.exists(file_path):
        logging.info(f"File 'index.html' tidak ditemukan. Membuat file baru di '{file_path}'")
        with open(file_path, "w") as f:
            f.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Gateway Realtime Data</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; margin: 20px; background-color: #f8f9fa; color: #212529; }
        h1 { color: #0056b3; }
        table { border-collapse: collapse; width: 95%; margin-top: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); background-color: white;}
        th, td { border: 1px solid #dee2e6; padding: 12px; text-align: left; }
        th { background-color: #007bff; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        tr:hover { background-color: #e9ecef; }
        td:nth-child(2) { font-family: monospace; font-size: 0.9em; word-break: break-all; }
        .status { font-weight: bold; padding: 5px 10px; border-radius: 5px; color: white; }
        #status.connected { background-color: #28a745; }
        #status.disconnected { background-color: #dc3545; }
        .highlight { animation: highlight-animation 1s ease-out; }
        @keyframes highlight-animation {
            from { background-color: #ffc107; }
            to { background-color: inherit; }
        }
    </style>
</head>
<body>
    <h1>IEC 61850 Realtime Data</h1>
    <p>Status: <span id="status" class="status disconnected">Connecting...</span></p>
    <table id="data-table">
        <thead>
            <tr>
                <th>IOA</th>
                <th>IEC 61850 Address</th>
                <th>Value</th>
                <th>Last Update</th>
            </tr>
        </thead>
        <tbody>
            </tbody>
    </table>

    <script>
        const tableBody = document.querySelector("#data-table tbody");
        const statusSpan = document.getElementById("status");

        function connect() {
            const socket = new WebSocket(`ws://${window.location.hostname}:8001`);

            socket.onopen = function(e) {
                console.log("[open] Koneksi berhasil dibuat");
                statusSpan.textContent = "Connected";
                statusSpan.className = "status connected";
            };

            socket.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    
                    if (data.type !== 'data_update' && data.type !== 'invalidation') {
                        return;
                    }

                    let row = document.getElementById(`ioa-${data.ioa}`);
                    if (!row) {
                        const rows = Array.from(tableBody.querySelectorAll('tr'));
                        const newIOA = parseInt(data.ioa, 10);
                        let inserted = false;
                        for (const r of rows) {
                            const currentIOA = parseInt(r.id.split('-')[1], 10);
                            if (newIOA < currentIOA) {
                                row = tableBody.insertBefore(document.createElement('tr'), r);
                                inserted = true;
                                break;
                            }
                        }
                        if (!inserted) {
                            row = tableBody.insertRow();
                        }
                        
                        row.id = `ioa-${data.ioa}`;
                        row.innerHTML = `<td>${data.ioa}</td><td></td><td></td><td></td>`;
                    }
                    
                    const addressCell = row.cells[1];
                    const valueCell = row.cells[2];
                    const timestampCell = row.cells[3];

                    addressCell.textContent = data.address || "N/A";

                    if (data.type === 'data_update') {
                        valueCell.textContent = typeof data.value === 'number' ? data.value.toFixed(2) : data.value;
                        valueCell.style.color = 'black';
                    } else if (data.type === 'invalidation') {
                        valueCell.textContent = 'INVALID';
                        valueCell.style.color = 'red';
                    }
                    
                    timestampCell.textContent = data.timestamp;

                    row.classList.remove('highlight');
                    void row.offsetWidth;
                    row.classList.add('highlight');

                } catch (error) {
                    console.error("Gagal mem-parsing data JSON:", error);
                }
            };

            socket.onclose = function(event) {
                console.log('[close] Koneksi ditutup. Mencoba menghubungkan ulang...');
                statusSpan.textContent = "Disconnected. Retrying in 5 seconds...";
                statusSpan.className = "status disconnected";
                setTimeout(connect, 5000);
            };

            socket.onerror = function(error) {
                console.error(`[error] ${error.message}`);
                statusSpan.textContent = "Connection Error";
                statusSpan.className = "status disconnected";
            };
        }

        document.addEventListener('DOMContentLoaded', connect);
    </script>
</body>
</html>
            """)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Gateway dimatikan karena KeyboardInterrupt.")
    except Exception as e:
        logging.critical(f"Terjadi error fatal di level atas: {e}", exc_info=True)
