from flask import Flask, request, jsonify, send_from_directory
import time
import threading
from datetime import datetime
from collections import defaultdict
import os
import socket  

app = Flask(__name__)

# Данные в памяти
current_server_data = {"time": "00:00:00"}
clients = defaultdict(lambda: {"count": 0, "lastRequest": None})

# --- АВТОМАТИЧЕСКАЯ РЕГИСТРАЦИЯ ВСЕХ IP ---
@app.before_request
def track_all_ips():
    # Игнорируем запросы к статике, если не хотим захламлять (по желанию)
    # if request.path.endswith(('.js', '.css', '.png')): return
    
    user_ip = request.remote_addr
    clients[user_ip]["lastRequest"] = datetime.now()
    clients[user_ip]["count"] += 1

@app.before_request
def track_all_ips():
    user_ip = request.remote_addr
    
    # Если IP новый или у него еще нет имени, пытаемся его узнать
    if "hostname" not in clients[user_ip]:
        try:
            # Пытаемся получить имя компьютера по IP
            hostname = socket.gethostbyaddr(user_ip)[0]
            clients[user_ip]["hostname"] = hostname
        except Exception:
            # Если не получилось (нет в DNS), пишем "Неизвестно"
            clients[user_ip]["hostname"] = "Unknown Device"

    clients[user_ip]["lastRequest"] = datetime.now()
    clients[user_ip]["count"] += 1

# --- ФОНОВАЯ ЗАДАЧА (Обновление времени) ---
def update_time_loop():
    global current_server_data
    while True:
        try:
            now_str = datetime.now().strftime("%H:%M:%S")
            current_server_data["time"] = now_str
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(5)

threading.Thread(target=update_time_loop, daemon=True).start()

# --- РОУТЫ ---

@app.route('/vremja.json')
def get_vremja_json():
    response = jsonify(current_server_data)
    return make_no_cache_response(response)

@app.route('/check', methods=['POST'])
def check_password():
    try:
        data = request.get_json()
    except:
        return jsonify({"error": "Invalid JSON"}), 400

    # Пароль теперь только для того, чтобы увидеть список
    if data and data.get("password") == "6561":
        active_clients = []
        inactive_clients = []
        now = datetime.now()
        
        for ip, info in clients.items():
            last = info["lastRequest"]
            is_active = last and (now - last).total_seconds() < 30
            
            client_info = {
                "ip": ip,
                "hostname": info.get("hostname", "Поиск..."), # Добавляем имя
                "count": info["count"],
                "lastRequest": last.strftime("%H:%M:%S") if last else "Никогда"
            }

            if is_active:
                active_clients.append(client_info)
            else:
                inactive_clients.append(client_info)
        
        return jsonify({
            "active": active_clients,
            "inactive": inactive_clients
        })
    return jsonify({"error": "Неверный пароль"}), 401

@app.route('/')
def index():
    return make_no_cache_response(send_from_directory(os.getcwd(), 'index.html'))

@app.route('/<path:filename>')
def serve_static(filename):
    return make_no_cache_response(send_from_directory(os.getcwd(), filename))

def make_no_cache_response(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

if __name__ == '__main__':
    print("🚀 Мониторинг запущен на порту 80")
    app.run(host='0.0.0.0', port=80, debug=False)
