import asyncio
import logging
import time
import sys
import json  # <--- ДОБАВЛЕНО ДЛЯ РАБОТЫ С JSON
import glob  # <--- ДОБАВЛЕНО ДЛЯ ОЧИСТКИ ЛОГОВ
from datetime import datetime
from typing import Optional, Dict, List
import configparser
import pyodbc
from pymcprotocol import Type3E
import os
import threading

# Глобальные переменные для хранения состояния 
_prev_c110 = None
_last_formatted_text = ""

# ========================== НАСТРОЙКА ЛОГИРОВАНИЯ ==========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("plc_monitor.log", encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ========================== ЧТЕНИЕ КОНФИГУРАЦИИ ==========================
config_text = """
[PLC]
ip = 192.168.161.1
ports = 5000,5001,5002,5003,5004
timeout = 5
poll_interval = 0.3

[Database]
server = tcp:127.0.0.1\\OITNK,1433
database = yamid
user = klient
password = 1234567
driver = {ODBC Driver 17 for SQL Server}
"""

config = configparser.ConfigParser()
config.read_string(config_text)

PLC_IP = config.get("PLC", "ip")
PLC_PORTS = [int(p) for p in config.get("PLC", "ports").split(",")] 
PLC_TIMEOUT = config.getfloat("PLC", "timeout")
POLL_INTERVAL = config.getfloat("PLC", "poll_interval")

DB_SERVER = config.get("Database", "server")
DB_NAME = config.get("Database", "database")
DB_USER = config.get("Database", "user")
DB_PASS = config.get("Database", "password")
DB_DRIVER = config.get("Database", "driver")

SQL_CONN_STR = (
    f"Driver={DB_DRIVER};Server={DB_SERVER};Database={DB_NAME};"
    f"UID={DB_USER};PWD={DB_PASS};"
)

# ========================== КОНФИГУРАЦИЯ ЗОН ==========================
ZONES_CONFIG = [
    {"name": "cep_inf",  "x_idx": 18, "invert": False},
    # Х22 - ЦЕПЬ
    {"name": "inf1_",    "x_idx": 24, "invert": False},
    # X30   ЗАКЛАДКА
    {"name": "inf2_",    "x_idx": 25, "invert": False},
    # Х31   МОСТ
    {"name": "inf3_",    "x_idx": 26, "invert": False},
    # Х32   КАРДАН
    {"name": "inf4_",    "x_idx": 27, "invert": False},
    # Х33   ПЕРЕВАРОТ
    {"name": "inf5_",    "x_idx": 28, "invert": False},
    # Х34   ДВИГАТЕЛЬ
    {"name": "inf6_",    "x_idx": 29, "invert": False},
    # Х35   РАДИАТОР
    {"name": "inf7_",    "x_idx": 31, "invert": False},
    # Х37   КАБИНА
    {"name": "inf8_",    "x_idx": 32, "invert": False},
    # Х40   НАДРАМНИК
    {"name": "inf9_",    "x_idx": 33, "invert": False},
    # Х41   ЗАПРАВКА
    {"name": "inf10_",   "x_idx": 34, "invert": False},
    # Х42   СПУСК
    {"name": "inf11_",   "x_idx": 30, "invert": False},
    # Х36   ПОДВАЛ
    {"name": "inf12_",   "y_idx": 5,  "invert": True},
    # У5   КОНТРОЛЛЕР
]

C_MAPPING_ID2 = [46, 30, 31, 32, 33, 34, 35, 37, 40, 41, 42, 36, 44]

# ========================== КЛАСС ЗОНЫ ==========================
class Zone:
    def __init__(self, name: str, x_idx: int = None, y_idx: int = None, invert: bool = False):
        self.name = name
        self.x_idx = x_idx
        self.y_idx = y_idx
        self.invert = invert
        self.active = False
        self.prev_active = False
        self.current_time = 0.0

    def update(self, signal_state: bool, dt: float):
        is_active = (not signal_state) if self.invert else signal_state
        if is_active and not self.prev_active:
            self.current_time = 0.0
        if is_active:
            self.current_time += dt
        self.prev_active = is_active
        self.active = is_active

# ========================== УТИЛИТЫ ==========================
def format_time(seconds: float) -> str:
    if seconds is None:
        return "NULL"
    try:
        s = int(float(seconds))
    except (ValueError, TypeError):
        return "NULL"
    if s < 0:
        s = 0
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}:{m:02d}:{sec:02d}"

def get_table_name() -> str:
    return "data_on_line" + datetime.now().strftime("%Y_%m_%d")

# Убедись, что в начале файла есть: import pyodbc

def get_graf_table_name() -> str:
    return "graf_" + datetime.now().strftime("%Y_%m_%d")



# Маппинг зон на реальные колонки в БД (порядок записи в insert_graf_row)
ZONE_TO_DB_COLUMN = {
    'cep_inf': 'cep_inf',
    'inf1_': 'inf1_',
    'inf2_': 'inf2_',
    'inf3_': 'inf3_',
    'inf4_': 'inf4_',
    'inf5_': 'inf5_',
    'inf6_': 'inf6_',
    'inf7_': 'inf8_',   # inf7_ пишется в колонку inf8_
    'inf8_': 'inf9_',   # inf8_ пишется в колонку inf9_
    'inf9_': 'inf10_',  # inf9_ пишется в колонку inf10_
    'inf10_': 'inf11_', # inf10_ пишется в колонку inf11_
    'inf11_': 'inf7_',  # inf11_ пишется в колонку inf7_
    'inf12_': 'inf12_',
}

def count_stops_in_graf() -> Dict[str, int]:
    """Подсчитывает количество остановов (переходов 0→1) для каждой зоны из таблицы graf"""
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        graf_table_name = get_graf_table_name()
        
        # Читаем из реальных колонок, в которые пишутся данные
        db_columns = [ZONE_TO_DB_COLUMN[zone] for zone in ZONE_TO_DB_COLUMN.keys()]
        query = f"""
            SELECT {', '.join(db_columns)}
            FROM {graf_table_name}
            ORDER BY Id
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Словарь для подсчета (ключи - имена зон)
        stop_counts = {zone: 0 for zone in ZONE_TO_DB_COLUMN.keys()}
        
        prev_values = {zone: 0 for zone in ZONE_TO_DB_COLUMN.keys()}
        
        for row in rows:
            for idx, zone in enumerate(ZONE_TO_DB_COLUMN.keys()):
                current_value = int(row[idx]) if row[idx] is not None else 0
                if prev_values[zone] == 0 and current_value == 1:
                    stop_counts[zone] += 1
                prev_values[zone] = current_value
        
        return stop_counts
    except Exception as e:
        logger.error(f"Ошибка подсчета остановов: {e}")
        return {zone: 0 for zone in ZONE_TO_DB_COLUMN.keys()}


# Глобальные переменные для двойной буферизации
_JSON_BUFFER_A = 'plc_state_A.json'
_JSON_BUFFER_B = 'plc_state_B.json'
_JSON_ACTIVE_FILE = 'plc_state.json'  # Символическая ссылка на активный буфер
_current_buffer = 'A'  # Начинаем с буфера A

def generate_and_save_json(zones: List[Zone], plc_data: Dict, current_m45: int):
    """Формирует и сохраняет актуальное состояние системы в JSON файл (двойная буферизация)."""
    global _current_buffer
    
    try:
        c_vals = plc_data.get("C", [])
        c110 = c_vals[80] if len(c_vals) > 80 else 0
        c50 = c_vals[20] if len(c_vals) > 20 else 0
        c45 = c_vals[15] if len(c_vals) > 15 else 0
        c46 = c_vals[16] if len(c_vals) > 16 else 0
        
        stop_counts = count_stops_in_graf()
        
        c_register_map = {
            'cep_inf': {'idx': 16, 'name': 'C46'},
            'inf1_': {'idx': 0, 'name': 'C30'},
            'inf2_': {'idx': 1, 'name': 'C31'},
            'inf3_': {'idx': 2, 'name': 'C32'},
            'inf4_': {'idx': 3, 'name': 'C33'},
            'inf5_': {'idx': 4, 'name': 'C34'},
            'inf6_': {'idx': 5, 'name': 'C35'},
            'inf7_': {'idx': 7, 'name': 'C37'},
            'inf8_': {'idx': 10, 'name': 'C40'},
            'inf9_': {'idx': 11, 'name': 'C41'},
            'inf10_': {'idx': 12, 'name': 'C42'},
            'inf11_': {'idx': 6, 'name': 'C36'},
            'inf12_': {'idx': 14, 'name': 'C44'},
        }

        d2000 = plc_data.get("D2000", [])
        current_bookmark = "НЕТ ЗАКЛАДКИ!!!"
        if len(d2000) >= 15:
            text = decode_words_to_string(d2000[:14], 14)
            int_val = d2000[14]
            if text.strip() == "" and int_val == 0:
                current_bookmark = "НЕТ ЗАКЛАДКИ!!!"
            else:
                current_bookmark = f"{text} {int_val}".strip()

        state = {
            "timestamp": datetime.now().isoformat(),
            "system": {
                "c110": c110,
                "c50_formatted": format_time(c50),
                "c45_formatted": format_time(c45),
                "c46_formatted": format_time(c46),
                "m45_state": current_m45,
                "m45_allowed": current_m45 == 1
            },
            "zones": [
                {
                    "name": z.name,
                    "active": z.active,
                    "current_time": format_time(z.current_time) if z.active else "0:00:00",
                    "total_time": format_time(c_vals[c_register_map[z.name]['idx']]) if z.name in c_register_map and c_register_map[z.name]['idx'] < len(c_vals) else "0:00:00",
                    "c_register": c_register_map.get(z.name, {}).get('name', ''),
                    "c_value": c_vals[c_register_map[z.name]['idx']] if z.name in c_register_map and c_register_map[z.name]['idx'] < len(c_vals) else 0,
                    "stop_count": stop_counts.get(z.name, 0),
                    "x_idx": z.x_idx,
                    "y_idx": z.y_idx
                }
                for z in zones
            ],
            "current_bookmark": current_bookmark,
            "D714": plc_data.get("D714", 0),   # 🔥 Для Flask событий
            "D4000": plc_data.get("D4000", 0), # 🔥 Для Flask событий
            "X": plc_data.get("X", []),
            "Y": plc_data.get("Y", []),
            "L": plc_data.get("L", []),
            "M": plc_data.get("M", []),
            "C": plc_data.get("C", []),
        }

        # 🔥 ДВОЙНАЯ БУФЕРИЗАЦИЯ
        target_file = _JSON_BUFFER_A if _current_buffer == 'A' else _JSON_BUFFER_B
        
        # Пишем в неактивный буфер
        tmp_filename = target_file + ".tmp"
        with open(tmp_filename, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        
        # Атомарная замена (теперь безопасно, т.к. Flask читает ДРУГОЙ файл)
        os.replace(tmp_filename, target_file)
        
        # Переключаем активный буфер
        _current_buffer = 'B' if _current_buffer == 'A' else 'A'
        active_file = _JSON_BUFFER_A if _current_buffer == 'A' else _JSON_BUFFER_B
        
        # Обновляем символическую ссылку (или просто копируем имя)
        # На Windows лучше просто записывать имя активного файла
        with open(_JSON_ACTIVE_FILE, "w", encoding="utf-8") as f:
            json.dump({"active": active_file, "timestamp": datetime.now().isoformat()}, f)
        
    except Exception as e:
        logger.error(f"❌ Ошибка записи JSON состояния: {e}")

def get_ist_ostan_table_name() -> str:
    return "ist_ostan_" + datetime.now().strftime("%Y_%m_%d")


# ========================== СОЗДАНИЕ ТАБЛИЦ ==========================
def ensure_tables_exist():
    """Создает все три таблицы на сегодня, если их нет."""
    table_name = get_table_name()
    graf_table_name = get_graf_table_name()
    ist_table_name = get_ist_ostan_table_name()
    date_str = datetime.now().strftime("%d-%m-%Y")
    
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        # Таблица 1: data_on_line (3 фиксированные строки)
        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NULL
            BEGIN
                CREATE TABLE {table_name} (
                    Id INT PRIMARY KEY,
                    _data NVARCHAR(50),
                    cep_inf NVARCHAR(50),
                    inf1_ NVARCHAR(50), inf2_ NVARCHAR(50), inf3_ NVARCHAR(50),
                    inf4_ NVARCHAR(50), inf5_ NVARCHAR(50), inf6_ NVARCHAR(50),
                    inf7_ NVARCHAR(50), inf8_ NVARCHAR(50), inf9_ NVARCHAR(50),
                    inf10_ NVARCHAR(50), inf11_ NVARCHAR(50), inf12_ NVARCHAR(50)
                )
                INSERT INTO {table_name} (Id, _data) VALUES (1, '{date_str}')
                INSERT INTO {table_name} (Id, _data) VALUES (2, '{date_str}')
                INSERT INTO {table_name} (Id, _data) VALUES (3, NULL)
            END
        """)
        
        # Таблица 2: graf (автоинкремент Id)
        cursor.execute(f"""
            IF OBJECT_ID('{graf_table_name}', 'U') IS NULL
            BEGIN
                CREATE TABLE {graf_table_name} (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    _data text NULL,
                    cep_inf text NULL,
                    inf1_ text NULL, inf2_ text NULL, inf3_ text NULL,
                    inf4_ text NULL, inf5_ text NULL, inf6_ text NULL,
                    inf7_ text NULL, inf8_ text NULL, inf9_ text NULL,
                    inf10_ text NULL, inf11_ text NULL, inf12_ text NULL
                )
                INSERT INTO {graf_table_name} (_data) VALUES ('{date_str}')
            END
        """)
        
        # Таблица 3: ist_ostan (200 строк) 
        cursor.execute(f"""
            IF OBJECT_ID('{ist_table_name}', 'U') IS NULL
            BEGIN
                CREATE TABLE {ist_table_name} (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    na_lente text NULL,
                    tek_stop text NULL
                )
            END
        """)
        
        # Проверяем количество строк и добавляем недостающие до 200
        cursor.execute(f"SELECT COUNT(*) FROM {ist_table_name}")
        current_count = cursor.fetchone()[0]
        
        if current_count < 200:
            rows_to_add = 200 - current_count
            logger.info(f"⚠️ Таблица {ist_table_name} имеет {current_count} строк. Добавляем {rows_to_add} строк...")
            
            for _ in range(rows_to_add):
                cursor.execute(f"INSERT INTO {ist_table_name} (na_lente, tek_stop) VALUES (NULL, NULL)")
            
            conn.commit()
            logger.info(f"✅ Добавлено {rows_to_add} строк. Теперь в таблице 200 строк.")
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"✅ Все таблицы созданы/проверены")
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблиц: {e}")


# ========================== ФАЙЛ ДЛЯ ТАЙМЛАЙН ВИЗУАЛИЗАЦИИ ==========================
# ========================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ ТАЙМЛАЙНА ==========================
TIMELINE_LOG_BASE = r"c:/log/timeline_log"
TIMELINE_LOG_LOCK = threading.Lock()
TIMELINE_LOG_RETENTION_DAYS = 10

# Храним состояние ТОЛЬКО дискретных сигналов для сравнения
_prev_discrete_state = {
    "X": [], "Y": [], "L": [], "M": []
}
_last_timeline_log_time = 0.0
_LOG_INTERVAL_SEC = 1.0  # Принудительная запись каждые 1 секунду (идеально для счетчиков и графика)

def get_timeline_log_path(date=None):
    """Возвращает путь к лог-файлу timeline за конкретную дату"""
    if date is None:
        date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    return f"{TIMELINE_LOG_BASE}_{date_str}.jsonl"

def cleanup_old_timeline_logs(keep_days=TIMELINE_LOG_RETENTION_DAYS):
    """Удаляет старые timeline лог-файлы"""
    try:
        today = datetime.now().date()
        log_dir = os.path.dirname(TIMELINE_LOG_BASE) or "."
        if not os.path.exists(log_dir):
            return []
        deleted = []
        pattern = os.path.join(log_dir, f"{os.path.basename(TIMELINE_LOG_BASE)}_*.jsonl")
        for filepath in glob.glob(pattern):
            try:
                filename = os.path.basename(filepath)
                date_part = filename.replace(f"{os.path.basename(TIMELINE_LOG_BASE)}_", "").replace(".jsonl", "")
                file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                if (today - file_date).days >= keep_days:
                    os.remove(filepath)
                    deleted.append(filename)
                    logger.info(f"🗑️ Удален старый timeline лог: {filename}")
            except (ValueError, Exception):
                continue
        return deleted
    except Exception as e:
        logger.error(f"Ошибка очистки timeline логов: {e}")
        return []

def write_timeline_log(plc_data: Dict):
    """ГИБРИДНАЯ ЗАПИСЬ с ленивой проверкой целостности данных"""
    global _prev_discrete_state, _last_timeline_log_time
    
    current_C = plc_data.get("C", [])
    
    # ОПТИМИЗАЦИЯ: any() вместо sum(). Остановится на первом же ненулевом значении.
    if current_C and len(current_C) > 20:
        if not any(current_C[:20]):  # Быстрее, чем sum() == 0
            logger.warning("⚠️ Обнаружены нулевые счетчики (таймаут?). Пропускаем запись.")
            return

    current_X = plc_data.get("X", [])
    current_Y = plc_data.get("Y", [])
    current_L = plc_data.get("L", [])
    current_M = plc_data.get("M", [])
    current_time = time.time()

    discrete_changed = (
        current_X != _prev_discrete_state["X"] or 
        current_Y != _prev_discrete_state["Y"] or 
        current_L != _prev_discrete_state["L"] or 
        current_M != _prev_discrete_state["M"]
    )

    needs_heartbeat = (current_time - _last_timeline_log_time) >= _LOG_INTERVAL_SEC

    if discrete_changed or needs_heartbeat:
        log_entry = {
            # ОПТИМИЗАЦИЯ: Убран избыточный вызов datetime для ISO-строки, f-строка быстрее
            "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3], 
            "X": current_X, "Y": current_Y, "L": current_L, "M": current_M,
            "C": current_C,
            "D100": plc_data.get("D100", 0)
        }
        
        log_path = get_timeline_log_path()
        # ВНИМАНИЕ: os.makedirs(os.path.dirname(log_path)) лучше вызвать ОДИН раз при запуске скрипта!

        try:
            with TIMELINE_LOG_LOCK:
                with open(log_path, "a", encoding="utf-8") as f:
                    # separators убирает пробелы. f.flush() убран, так как сработает автоматом при выходе
                    f.write(json.dumps(log_entry, ensure_ascii=False, separators=(',', ':')) + "\n")
            
            _prev_discrete_state = {"X": current_X, "Y": current_Y, "L": current_L, "M": current_M}
            _last_timeline_log_time = current_time
        except Exception as e:
            logger.error(f"❌ Ошибка записи timeline лога: {e}")

# ========================== СБОР ДАННЫХ С ПЛК ==========================
def red_slmp(read_rd: bool = False, max_retries: int = 3) -> Optional[Dict]:
    """Опрашивает ПЛК с повторными попытками при таймауте."""
    mc = None
    start_time = time.perf_counter()
    
    for attempt in range(max_retries):
        try:
            port = PLC_PORTS[0]
            mc = Type3E()
            mc.connect(PLC_IP, port)
            if hasattr(mc, '_sock') and mc._sock:
                mc._sock.settimeout(5.0)  # Увеличен с 2 до 5 секунд

            # Читаем битовые регистры
            rx = mc.batchread_bitunits("X0", 43)
            ry = mc.batchread_bitunits("Y0", 6)
            rl = mc.batchread_bitunits("L0", 58)
            rm = mc.batchread_bitunits("M0", 528)
            
            # Читаем счетчики (C30-C140 = 111 слов)
            rc = mc.batchread_wordunits("CN30", 111)
            
            d7016 = mc.batchread_wordunits("D7016", 496)
            d2000 = mc.batchread_wordunits("D2000", 16)
            d700 = mc.batchread_wordunits("D700", 16)
            # 🔥 ДОБАВЛЕНО: Чтение регистров, нужных для событий во Flask
            d714 = mc.batchread_wordunits("D714", 1)[0]
            d4000 = mc.batchread_wordunits("D4000", 1)[0]
            d100 = mc.batchread_wordunits("D100", 1)[0]  #  Расстояние между стойками
            # ==========================================
            all_data_rd = []
            
            if read_rd:
                c110 = rc[80] if len(rc) > 80 else 0
                needed_records = 33 + c110
                total_words_needed = needed_records * 24
                total_words_needed = min(total_words_needed, 31200)
                
                CHUNK_SIZE = 450
                for i in range(0, total_words_needed, CHUNK_SIZE):
                    size = min(CHUNK_SIZE, total_words_needed - i)
                    chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"R{i}", size)]
                    all_data_rd.extend(chunk)
                    time.sleep(0.002)

            result = {
                "X": [bool(x) for x in rx],
                "Y": [bool(y) for y in ry],
                "L": [bool(l) for l in rl],
                "M": [bool(m) for m in rm],
                "C": rc,
                "D7016": d7016,
                "D2000": d2000,
                "D700": d700,
                "D714": d714,
                "D4000": d4000,
                "D100": d100,  # 🔥 ДОБАВИТЬ
                "RD": all_data_rd
            }
            
            # Записываем в timeline лог для визуализации
            write_timeline_log(result)
            
            if attempt > 0:
                logger.info(f"✅ Успешное чтение после {attempt + 1} попытки")
            
            return result
            
        except Exception as e:
            elapsed_time = time.perf_counter() - start_time
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Попытка {attempt + 1}/{max_retries} не удалась (прошло {elapsed_time:.3f} сек.): {e}")
                time.sleep(0.5)  # Пауза перед повторной попыткой
            else:
                logger.error(f"❌ Ошибка опроса ПЛК после {max_retries} попыток (прошло {elapsed_time:.3f} сек.): {e}")
        finally:
            if mc:
                try:
                    mc.close()
                except:
                    pass
                mc = None
    
    return None

# ========================== ОБНОВЛЕНИЕ ТАБЛИЦ ==========================
def update_data_on_line(zones: List[Zone], plc_data: Dict):
    """Обновляет таблицу data_on_line (Id=1,2,3)."""
    table_name = get_table_name()
    c_vals = plc_data.get("C", [])
    
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        cols_list = ["cep_inf", "inf1_", "inf2_", "inf3_", "inf4_", "inf5_", 
                     "inf6_", "inf7_", "inf8_", "inf9_", "inf10_", "inf11_", "inf12_"]
        set_clause = ', '.join([f"{col}=?" for col in cols_list])
        
        row1_vals = [format_time(z.current_time) if z.active else "___" for z in zones]
        cursor.execute(f"UPDATE {table_name} SET {set_clause} WHERE Id=1", row1_vals)
        
        row2_vals = []
        for c_num in C_MAPPING_ID2:
            idx = c_num - 30
            if 0 <= idx < len(c_vals):
                row2_vals.append(format_time(c_vals[idx]))
            else:
                row2_vals.append("NULL")
        cursor.execute(f"UPDATE {table_name} SET {set_clause} WHERE Id=2", row2_vals)
        
        c110 = c_vals[80] if len(c_vals) > 80 else 0
        c50 = c_vals[20] if len(c_vals) > 20 else 0
        c45 = c_vals[15] if len(c_vals) > 15 else 0
        cursor.execute(f"UPDATE {table_name} SET _data=?, inf1_=?, inf2_=? WHERE Id=3", 
                       (str(c110), format_time(c50), format_time(c45)))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка обновления data_on_line: {e}")

def insert_graf_row(zones: List[Zone]):
    """Добавляет новую строку в таблицу graf."""
    graf_table_name = get_graf_table_name()
    time_str = datetime.now().strftime("%H:%M:%S")
    
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        zone_values = ["1" if z.active else "0" for z in zones]
        # Порядок колонок соответствует C# приложению (нельзя менять!)
        cols = "cep_inf, inf1_, inf2_, inf3_, inf4_, inf5_, inf6_, inf8_, inf9_, inf10_, inf11_, inf7_, inf12_"
        placeholders = ','.join(['?' for _ in range(13)])
        
        sql = f"INSERT INTO {graf_table_name} (_data, {cols}) VALUES (?, {placeholders})"
        cursor.execute(sql, [time_str] + zone_values)
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка INSERT в graf: {e}")

def decode_words_to_string(words: List[int], count: int) -> str:
    """Декодирует список слов (по 2 байта) в строку символов."""
    chars = []
    for i in range(count):
        if i < len(words):
            low = words[i] & 0xFF
            high = (words[i] >> 8) & 0xFF
            if low == 0:
                low = 32
            if high == 0:
                high = 32
            chars.append(chr(low))
            chars.append(chr(high))
    return ''.join(chars).rstrip()

def decode_rd_record(rd_data: List[int], index: int) -> Optional[Dict]:
    """Декодирует ПОЛНУЮ архивную запись из RD регистров ПЛК """
    offset = index * 24
    if offset + 23 >= len(rd_data):
        return None
    
    record = rd_data[offset:offset+24]
    if all(v == 0 for v in record):
        return None
    
    try:
        year    = record[0]
        month   = record[1]
        day     = record[2]
        hour    = record[3]
        minute  = record[4]
        second  = record[5]
        release_num = record[6]
        distance    = record[7]
        
        model_bytes = record[8:22]
        model_text = decode_words_to_string(model_bytes, 14).strip()
        links_val = record[22]
        
        dt_str = f"{day:02d}.{month:02d}.{year} {hour:02d}:{minute:02d}:{second:02d}"
        
        extended_string = (
            f"[{dt_str}] {release_num:<2} | "
            f" {distance:<2} | "
            f"{model_text} |  {links_val}"
        )
        
        return {"model": extended_string}
    except Exception as e:
        logger.error(f"Ошибка парсинга записи RD на индексе {index}: {e}")
        return None


def update_ist_ostan(d7016: List[int], d700: List[int], d2000: List[int], rd_data: List[int], c110: int, signals: Dict[str, bool]):
    """Обновляет таблицу истории остановов (na_lente + tek_stop)."""
    ist_table_name = get_ist_ostan_table_name()
    
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        lenta_entries = []
        tek_stop_list = []

        if len(d2000) >= 15:
            text = decode_words_to_string(d2000[:14], 14)
            int_val = d2000[14]
            if text.strip() == "" and int_val == 0:
                lenta_entries.append("НЕТ ЗАКЛАДКИ!!!")
            else:
                lenta_entries.append(f"{text} {int_val}".strip())
        else:
            lenta_entries.append("НЕТ ЗАКЛАДКИ!!!")

        if len(d700) >= 15:
            text = decode_words_to_string(d700[:14], 14)
            int_val = d700[14]
            lenta_entries.append(f"{text} {int_val}".strip())
        else:
            lenta_entries.append("")

        for i in range(29 + c110):
            record = decode_rd_record(rd_data, i+2)  
            if record:
                prefix = i if i <= 28 else i - 28
                lenta_entries.append(f"{prefix}:<={record['model']}".strip())
            else:
                continue

        tek_stop_list.append("1" if signals.get("X22", False) else "0")
        tek_stop_list.append("1" if signals.get("X30", False) else "0")
        tek_stop_list.append("1" if signals.get("X31", False) else "0")
        tek_stop_list.append("1" if signals.get("X32", False) else "0")
        tek_stop_list.append("1" if signals.get("X33", False) else "0")
        tek_stop_list.append("1" if signals.get("X34", False) else "0")
        tek_stop_list.append("1" if signals.get("X35", False) else "0")
        tek_stop_list.append("1" if signals.get("X37", False) else "0")
        tek_stop_list.append("1" if signals.get("X40", False) else "0")
        tek_stop_list.append("1" if signals.get("X41", False) else "0")
        tek_stop_list.append("1" if signals.get("X42", False) else "0")
        tek_stop_list.append("1" if signals.get("X36", False) else "0")
        tek_stop_list.append("1" if not signals.get("Y5", True) else "0")
        tek_stop_list.append(str(c110))
        tek_stop_list.append(datetime.now().strftime("%H:%M"))
        
        while len(lenta_entries) < 200:
            lenta_entries.append("")
        while len(tek_stop_list) < 200:
            tek_stop_list.append("")

        cursor.execute(f"SELECT Id FROM {ist_table_name} ORDER BY Id")
        rows = cursor.fetchall()
        
        for idx, row in enumerate(rows):
            row_id = row[0]
            na_lente = lenta_entries[idx]
            tek_stop = tek_stop_list[idx]
            
            cursor.execute(
                f"UPDATE {ist_table_name} SET na_lente=?, tek_stop=? WHERE Id=?",
                (na_lente, tek_stop, row_id)
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.debug(f"✅ Таблица {ist_table_name} обновлена (C110={c110}, RD-записей: {29+c110})")
    except Exception as e:
        logger.error(f"❌ Ошибка обновления ist_ostan: {e}")
        if 'conn' in locals() and conn:
            try: conn.rollback()
            except: pass


def write_m45(value: int):
    """Записывает бит M45 в ПЛК"""
    mc = None
    try:
        mc = Type3E()
        mc.connect(PLC_IP, PLC_PORTS[0])
        if hasattr(mc, '_sock') and mc._sock:
            mc._sock.settimeout(5.0)
        mc.batchwrite_bitunits("M45", [value])
    except Exception as e:
        logger.error(f"❌ Ошибка записи M45: {e}")
    finally:
        if mc:
            try: mc.close()
            except: pass

# ========================== АСИНХРОННЫЙ ГЛАВНЫЙ ЦИКЛ ==========================
if sys.version_info >= (3, 9):
    to_thread = asyncio.to_thread
else:
    async def to_thread(func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)


async def main_loop():
    logger.info("🚀 Запуск скрипта с 3 таблицами и генерацией JSON...")
    await to_thread(ensure_tables_exist)
    
    zones = [Zone(z["name"], z.get("x_idx"), z.get("y_idx"), z["invert"]) for z in ZONES_CONFIG]
    last_m45_state = None  
    last_data_update = 0
    last_graf_update = 0
    last_lenta_update = 0
    last_tick_time = time.time()
    last_date_check = datetime.now().date()

    while True:
        current_time = time.time()
        dt = current_time - last_tick_time
        last_tick_time = current_time
        
        # ========================== ЛОГИКА M45 ==========================
        now = datetime.now()
        stop_shet = now.hour * 60 + now.minute

        # 7:00 это 420 мин, 11:00 это 660 мин
        # 11:20 это 680 мин, 15:20 это 920 мин
        if (420 <= stop_shet < 660) or (680 <= stop_shet < 920):
            current_m45 = 1
        else:
            current_m45 = 0

        if current_m45 != last_m45_state:
            await to_thread(write_m45, current_m45)
            last_m45_state = current_m45
            logger.info(f"🔄 M45 изменен на {current_m45} (время {now.hour:02d}:{now.minute:02d})")
            
        today = datetime.now().date()
        if today != last_date_check:
            logger.info(f"📅 Новый день: {today}")
            await to_thread(ensure_tables_exist)
            last_date_check = today
            for z in zones:
                z.current_time = 0.0
                z.prev_active = False
                z.active = False
        
        need_rd = (current_time - last_lenta_update >= 15.0)
        plc_data = await to_thread(red_slmp, read_rd=need_rd)
        
        if plc_data is None:
            logger.warning("⚠️ Данные от ПЛК не получены.")
            await asyncio.sleep(2)
            continue

        x_bits = plc_data["X"]
        y_bits = plc_data["Y"]
        
        for z in zones:
            if z.x_idx is not None:
                z.update(x_bits[z.x_idx], dt)
            elif z.y_idx is not None:
                z.update(y_bits[z.y_idx], dt)
                
        if current_time - last_data_update >= 1.0:
            await to_thread(update_data_on_line, zones, plc_data)
            
            # <--- ДОБАВЛЕНО: Генерация JSON для веб-интерфейса каждую секунду
            await to_thread(generate_and_save_json, zones, plc_data, current_m45)
            
            last_data_update = current_time
            
        if current_time - last_graf_update >= 10.0:
            await to_thread(insert_graf_row, zones)
            last_graf_update = current_time
            
        if need_rd:
            c_vals = plc_data.get("C", [])
            c110 = c_vals[80] if len(c_vals) > 80 else 0
            
            signals = {
                "X22": x_bits[18] if len(x_bits) > 18 else False,
                "X30": x_bits[24] if len(x_bits) > 24 else False,
                "X31": x_bits[25] if len(x_bits) > 25 else False,
                "X32": x_bits[26] if len(x_bits) > 26 else False,
                "X33": x_bits[27] if len(x_bits) > 27 else False,
                "X34": x_bits[28] if len(x_bits) > 28 else False,
                "X35": x_bits[29] if len(x_bits) > 29 else False,
                "X36": x_bits[30] if len(x_bits) > 30 else False,
                "X37": x_bits[31] if len(x_bits) > 31 else False,
                "X40": x_bits[32] if len(x_bits) > 32 else False,
                "X41": x_bits[33] if len(x_bits) > 33 else False,
                "X42": x_bits[34] if len(x_bits) > 34 else False,
                "Y5": y_bits[5] if len(y_bits) > 5 else False,
            }
            
            await to_thread(
                update_ist_ostan,
                plc_data["D7016"],
                plc_data["D700"],
                plc_data["D2000"],
                plc_data["RD"],
                c110,
                signals
            )
            last_lenta_update = current_time
            
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("🛑 Остановлено.")
