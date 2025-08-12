#!/usr/bin/env python3
# gateway_v17.0.py - Rilis Final dengan Arsitektur Full Async & Shutdown Sempurna
# Deskripsi: Gateway IEC 61850 ke IEC 60870-5-104.
# Fitur v17.0:
# - Arsitektur shutdown yang dirombak total menggunakan signal handler untuk 1x Ctrl+C.
# - Menggunakan aiohttp untuk integrasi penuh dengan asyncio.
# - Menggabungkan semua fitur UI dan logika yang sudah stabil.

import asyncio
import json
import logging
import configparser
import threading
import sys
import os
import time
from urllib.parse import urlparse
import websockets
from aiohttp import web
import signal

try:
    import libiec61850client_cached as libiec61850client
    import libiec60870server
    from lib60870 import *
    from lib61850 import IedConnection_getState
except ImportError as e:
    print(f"Error: Gagal mengimpor library yang dibutuhkan. Pesan: {e}")
    sys.exit(1)

# --- Definisikan konstanta & Variabel Global ---
FALLBACK_POLLING_INTERVAL, HEARTBEAT_POLLING_INTERVAL, RECONNECT_DELAY = 10, 60, 15
HTTP_PORT, WEBSOCKET_PORT = 8000, 8001

clients_dict_lock = threading.Lock()
ied_locks, ied_clients = {}, {}
ied_to_ioas_map, mms_to_ioa_map, ioa_inversion_map, ioa_to_mms_config, mms_to_value_path_map, ioa_to_full_address_map = {}, {}, {}, {}, {}, {}
processing_queue, broadcast_queue, iec104_server, main_loop = None, None, None, None
websocket_clients = set()
realtime_data_cache, connection_status_cache = {}, {}
cache_lock = asyncio.Lock()
shutdown_event = asyncio.Event()

# --- Fungsi-fungsi untuk Server Web (aiohttp) ---
async def handle_http_get(request):
    try:
        return web.FileResponse('./index.html')
    except Exception as e:
        logging.error(f"Gagal menyajikan index.html: {e}")
        return web.Response(status=404, text="index.html tidak ditemukan.")

async def start_aiohttp_server(port):
    app = web.Application()
    app.router.add_get('/', handle_http_get)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"HTTP server (aiohttp) berjalan di http://0.0.0.0:{port}")
    return runner

# --- (Sisa fungsi tidak berubah) ---
async def websocket_handler(websocket, path):
    logging.info(f"WebSocket client terhubung: {websocket.remote_address}")
    async with cache_lock:
        if connection_status_cache: await asyncio.gather(*[websocket.send(json.dumps(status)) for status in connection_status_cache.values()], return_exceptions=True)
        if realtime_data_cache: await asyncio.gather(*[websocket.send(json.dumps(data)) for data in realtime_data_cache.values()], return_exceptions=True)
    websocket_clients.add(websocket)
    try: await websocket.wait_closed()
    except websockets.exceptions.ConnectionClosed: logging.info(f"Koneksi WebSocket ditutup dari sisi client: {websocket.remote_address}")
    finally: logging.info(f"WebSocket client terputus: {websocket.remote_address}"); websocket_clients.remove(websocket)

async def broadcast_updates_task(queue):
    while not shutdown_event.is_set():
        try:
            update = await asyncio.wait_for(queue.get(), timeout=1.0)
            async with cache_lock:
                if update.get('type') == 'status_update': connection_status_cache[update['ied_id']] = update
                else: realtime_data_cache[update['ioa']] = update
            if websocket_clients: await asyncio.gather(*[client.send(json.dumps(update)) for client in websocket_clients], return_exceptions=True)
            queue.task_done()
        except asyncio.TimeoutError: continue
        except asyncio.CancelledError: break
    logging.info("Broadcast task dihentikan.")

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
    keys = path_str.split('.'); current_level = data_dict
    try:
        for key in keys: current_level = current_level[key]
        if isinstance(current_level, (int, float)): return float(current_level)
        return None
    except (KeyError, TypeError): return None

def process_data_update(ied_id, key, data):
    if not isinstance(data, dict) or 'value' not in data: return
    reported_key, value_to_update = key, data['value']
    mms_path_from_key = reported_key
    if "iec61850://" in reported_key:
        try: parsed_uri = urlparse(reported_key); mms_path_from_key = parsed_uri.path.lstrip('/')
        except Exception: logging.warning(f"Tidak dapat mem-parsing URI key: {reported_key}"); return
    valid_ioas_for_ied = set(ied_to_ioas_map.get(ied_id, []))
    if not valid_ioas_for_ied: return
    for config_path, ioa in mms_to_ioa_map.items():
        if ioa in valid_ioas_for_ied and config_path.startswith(mms_path_from_key):
            value_path = mms_to_value_path_map.get(config_path)
            final_value = get_value_by_path(value_to_update, value_path) if value_path else find_first_float(value_to_update)
            if final_value is None: continue
            try:
                ioa_type_class = iec104_server.IOA_list.get(ioa, {}).get('type')
                value_to_send = float(final_value)
                if "DoublePointInformation" in str(ioa_type_class): value_to_send = {1.0: 1, 2.0: 2}.get(value_to_send, 0)
                elif "SinglePointInformation" in str(ioa_type_class): value_to_send = 1 if int(value_to_send) != 0 else 0
                if ioa_inversion_map.get(ioa, False): value_to_send = {1: 2, 2: 1}.get(value_to_send, value_to_send)
                update_payload = {'type': 'data_update', 'ioa': ioa, 'value': value_to_send, 'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'), 'address': ioa_to_full_address_map.get(ioa, "N/A")}
                if main_loop and main_loop.is_running(): main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, update_payload)
                iec104_server.update_ioa(ioa, value_to_send)
                logging.info(f"[{ied_id}] Cocok '{reported_key}' -> IOA {ioa}, diperbarui dengan: {value_to_send}")
            except Exception as e: logging.error(f"Error saat memproses update untuk IOA {ioa}: {e}", exc_info=True)
            return

def do_invalidation(ied_id):
    if ied_id in ied_to_ioas_map:
        ioas_to_invalidate = ied_to_ioas_map[ied_id]
        logging.warning(f"Membuat invalid {len(ioas_to_invalidate)} titik data untuk {ied_id}.")
        for ioa in ioas_to_invalidate:
            update_payload = {'type': 'invalidation', 'ioa': ioa, 'value': 'INVALID', 'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'), 'address': ioa_to_full_address_map.get(ioa, "N/A")}
            if main_loop and main_loop.is_running(): main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, update_payload)

def ied_data_callback(key, data, ied_id):
    if main_loop and processing_queue and main_loop.is_running():
        update_item = {'type': 'process_data', 'ied_id': ied_id, 'key': key, 'data': data}
        main_loop.call_soon_threadsafe(processing_queue.put_nowait, update_item)

def invalidate_ied_points(ied_id):
    if main_loop and processing_queue and main_loop.is_running():
        update_item = {'type': 'invalidate', 'ied_id': ied_id}
        main_loop.call_soon_threadsafe(processing_queue.put_nowait, update_item)

def broadcast_connection_status(ied_id, status):
    if main_loop and broadcast_queue and main_loop.is_running():
        status_payload = {'type': 'status_update', 'ied_id': ied_id, 'status': status, 'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')}
        main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, status_payload)

async def data_processor_task(queue):
    while not shutdown_event.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
            if item.get('type') == 'process_data': await main_loop.run_in_executor(None, process_data_update, item['ied_id'], item['key'], item['data'])
            elif item.get('type') == 'invalidate': await main_loop.run_in_executor(None, do_invalidation, item['ied_id'])
            queue.task_done()
        except asyncio.TimeoutError: continue
        except asyncio.CancelledError: break
    logging.info("Data processor task dihentikan.")

async def ied_handler(ied_id, uris):
    logging.info(f"[{ied_id}] IED handler task dimulai.")
    client = None
    ied_locks[ied_id] = threading.Lock()
    def polling_entry_point(key, data): ied_data_callback(key, data, ied_id)
    def report_entry_point(key, data): ied_data_callback(key, data, ied_id)
    def locked_check_state():
        with ied_locks[ied_id]:
            if not client or not client.getRegisteredIEDs().get(ied_id, {}).get('con'): return False
            conn_info = client.getRegisteredIEDs()[ied_id]
            if conn_info.get('con'): return IedConnection_getState(conn_info['con']) == 2
            return False
    def locked_register_values():
        with ied_locks[ied_id]:
            for uri in uris: client.registerReadValue(str(uri))
            return len(client.polling)
    while not shutdown_event.is_set():
        try:
            logging.info(f"[{ied_id}] Mencoba menghubungkan...")
            broadcast_connection_status(ied_id, "CONNECTING")
            with ied_locks[ied_id]:
                client = libiec61850client.iec61850client(readvaluecallback=polling_entry_point, loggerRef=logging, cmdTerm_cb=None, Rpt_cb=report_entry_point)
            res = await main_loop.run_in_executor(None, client.getIED, ied_id.split(':')[0], int(ied_id.split(':')[1]))
            if res != 0: raise ConnectionError("getIED gagal")
            with clients_dict_lock: ied_clients[ied_id] = client
            logging.info(f"[{ied_id}] Koneksi berhasil.")
            broadcast_connection_status(ied_id, "CONNECTED")
            polling_item_count = await main_loop.run_in_executor(None, locked_register_values)
            active_polling_interval = HEARTBEAT_POLLING_INTERVAL if polling_item_count == 0 else FALLBACK_POLLING_INTERVAL
            logging.info(f"[{ied_id}] Mode: {'Heartbeat' if active_polling_interval == HEARTBEAT_POLLING_INTERVAL else 'Fallback'}. Interval: {active_polling_interval} detik.")
            while not shutdown_event.is_set():
                if not await main_loop.run_in_executor(None, locked_check_state):
                    raise ConnectionError("Koneksi terputus")
                await main_loop.run_in_executor(None, client.poll)
                await asyncio.sleep(0.1)
        except (ConnectionError, Exception) as e:
            if not shutdown_event.is_set(): logging.error(f"[{ied_id}] Handler error: {e}.")
        finally:
             if not shutdown_event.is_set():
                broadcast_connection_status(ied_id, "DISCONNECTED")
                with clients_dict_lock:
                    if ied_id in ied_clients: del ied_clients[ied_id]
                invalidate_ied_points(ied_id)
                logging.info(f"[{ied_id}] Menghubungkan ulang dalam {RECONNECT_DELAY} detik.")
                try: await asyncio.sleep(RECONNECT_DELAY)
                except asyncio.CancelledError: break
    logging.info(f"[{ied_id}] IED handler task berhenti.")

async def main_async():
    global iec104_server, main_loop, processing_queue, broadcast_queue, all_tasks, http_runner, websocket_server
    main_loop = asyncio.get_running_loop()
    processing_queue, broadcast_queue = asyncio.Queue(), asyncio.Queue()

    config = configparser.ConfigParser(); config.optionxform = str
    config.read(sys.argv[1] if len(sys.argv) > 1 else 'config.local.ini')
    
    http_runner = await start_aiohttp_server(HTTP_PORT)
    websocket_server = await websockets.serve(websocket_handler, "0.0.0.0", WEBSOCKET_PORT)
    
    iec104_server = libiec60870server.IEC60870_5_104_server()
    # ... (Konfigurasi parser tidak berubah) ...
    data_types = {'measuredvaluescaled': MeasuredValueScaled, 'measuredvaluefloat': MeasuredValueShort,
                  'singlepointinformation': SinglePointInformation, 'doublepointinformation': DoublePointInformation}
    command_types = {'singlepointcommand': SingleCommand, 'doublepointcommand': DoubleCommand}
    ied_data_groups = {}
    for section in list(data_types.keys()) + list(command_types.keys()):
        if section in config:
            for ioa, config_line in config[section].items():
                uri_part = config_line.split('#')[0]
                parsed = urlparse(uri_part)
                ied_id = f"{parsed.hostname}:{parsed.port or 102}"
                ioa_int = int(ioa)
                ioa_to_full_address_map[ioa_int] = config_line
                if ied_id not in ied_to_ioas_map: ied_to_ioas_map[ied_id] = []
                ied_to_ioas_map[ied_id].append(ioa_int)
                if section in data_types:
                    mms_to_ioa_map[parsed.path.lstrip('/')] = ioa_int
                    if '#' in config_line: mms_to_value_path_map[parsed.path.lstrip('/')] = config_line.split('#', 1)[1]
                    if ied_id not in ied_data_groups: ied_data_groups[ied_id] = []
                    ied_data_groups[ied_id].append(uri_part)
                if ':invers=true' in config_line: ioa_inversion_map[ioa_int] = True
                if section in command_types: ioa_to_mms_config[ioa_int] = config_line
    logging.info(f"Menemukan {len(ied_data_groups)} IED unik untuk dimonitor.")
    for section, mms_type in data_types.items():
        if section in config:
            for item in config[section]: iec104_server.add_ioa(int(item), mms_type, 0, None, True)
    for section, mms_type in command_types.items():
        if section in config:
            for item in config[section]: iec104_server.add_ioa(int(item), mms_type, 0, None, False)
    
    server_thread = threading.Thread(target=iec104_server.start, name="IEC104ServerThread", daemon=True)
    server_thread.start()
    
    all_tasks = [
        asyncio.create_task(data_processor_task(processing_queue)),
        asyncio.create_task(broadcast_updates_task(broadcast_queue)),
    ]
    all_tasks.extend([asyncio.create_task(ied_handler(ied_id, uris)) for ied_id, uris in ied_data_groups.items()])
    
    logging.info(f"Gateway v17.0 (Rilis Final) dimulai. Tekan Ctrl+C untuk berhenti.")
    await asyncio.gather(*all_tasks)

# PERUBAHAN: Fungsi shutdown yang bersih dan terkoordinasi
async def shutdown(sig, loop):
    logging.info(f"Diterima sinyal {sig.name}, memulai shutdown...")
    shutdown_event.set()

    # Beri waktu 1.5 detik bagi task untuk berhenti secara alami
    await asyncio.sleep(1.5)

    # Matikan server-server
    if 'http_runner' in globals() and http_runner:
        await http_runner.cleanup()
        logging.info("Server HTTP berhenti.")
    if 'websocket_server' in globals() and websocket_server:
        websocket_server.close()
        await websocket_server.wait_closed()
        logging.info("Server WebSocket berhenti.")
    if iec104_server:
        iec104_server.stop()
        logging.info("Server IEC 104 berhenti.")

    # Batalkan task yang mungkin masih berjalan
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("Semua task asyncio selesai.")
    loop.stop()


def create_index_html_if_not_exists():
    # ... (Tidak berubah, pastikan ada di file Anda)
    pass

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s', level=logging.INFO)
    create_index_html_if_not_exists()
    
    loop = asyncio.get_event_loop()
    
    # PERUBAHAN: Setup signal handler
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s, loop)))

    try:
        loop.create_task(main_async())
        loop.run_forever()
    finally:
        logging.info("Event loop ditutup.")
        loop.close()
