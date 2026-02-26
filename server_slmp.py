from pymodbus.client.sync import ModbusTcpClient
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pymcprotocol import Type3E
from flask import Flask, render_template, jsonify, request
import pandas as pd
import webbrowser
from sqlalchemy import create_engine
import socket
import logging
import warnings
from urllib.parse import quote_plus
from datetime import datetime, timedelta
warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy")
IP_ADDR = "192.168.161.1"
PORT  = 5000
PORT1 = 5001
PORT2 = 5002
PORT3 = 5003
PORT4 = 5004
IP = "192.168.161.1"
PORTS = [5000, 5001, 5002, 5003, 5004]
TOTAL_D = 8000
TOTAL_R = 31200
# Увеличиваем размер чанка до максимально стабильного для iQ-F/Q
CHUNK_SIZE = 450 #с большим значением не работает
TOTAL_WORDS_D = 8000
TOTAL_WORDS_RD = 31200
# Создаем глобальный кэш для данных ПЛК
TAGS = {
'SD': {
        0:   'Код ошибки самодиагностики',
        200: 'CPU switch status (0: RUN, 1: STOP)',
        201: 'LED Status',
        203: 'CPU Status (b0: RUN, b2: STOP, b3: PAUSE)',
        210: 'Clock Year',
        211: 'Clock Month',
        212: 'Clock Day',
        213: 'Clock Hour',
        214: 'Clock Minute',
        215: 'Clock Second',
        216: 'Clock Day of Week',
        519: 'Initial scan time (µs)',
        523: 'Minimum scan time (µs)',
        524: 'Maximum scan time (ms)',
        525: 'Maximum scan time (µs)',
        600: 'Memory Card Installation / Enable',
        604: 'SD memory card usage status',
        606: 'SD memory card capacity This register stores the drive 2 storage capacity (unit: 1 K byte).(Free space value after formatting is stored.)',
        607: 'SD memory card capacity This register stores the drive 2 storage capacity (unit: 1 K byte).(Free space value after formatting is stored.)',
        608: 'SD memory card capacity This register stores the drive 2 storage capacity (unit: 1 K byte).(Free space value after formatting is stored.)',
        609: 'SD memory card capacity This register stores the drive 2 storage capacity (unit: 1 K byte).(Free space value after formatting is stored.)',
        610: 'SD memory card free space capacity This register stores the free space value in drive 2 (unit: 1 K byte). ',
        611: 'SD memory card free space capacity This register stores the free space value in drive 2 (unit: 1 K byte). ',
        612: 'SD memory card free space capacity This register stores the free space value in drive 2 (unit: 1 K byte). ',
        613: 'SD memory card free space capacity This register stores the free space value in drive 2 (unit: 1 K byte). ',
    },
'RX': {  # Входящие сигналы (Input Signals)
        0:  'X0	    Резерв',
        1:  'X1	    Режим работы Выбран Контроллер/Реле',
        2:  'X2	    Резерв',
        3:  'X3	    Резерв',
        4:  'X4	    BQ21 набор шага и баз ',
        5:  'X5	    BQ22 набор шага и баз ',
        6:  'X6	    BQ37 Задняя тележка перед остановом ',
        7:  'X7	    BQ38 Задняятележка перед остановом ',
        8:  'X10	BQ15 открыт останов 1',
        9:  'X11	BQ16 закрыт останов 1',
        10: 'X12	BQ17 открыт останов 2',
        11: 'X13	BQ18 закрыт останов 2',
        12: 'X14	BQ9 передняя тележка перед остановом',
        13: 'X15	BQ10 передняя тележка перед остановом',
        14: 'X16	BQ35 закрытие останова',
        15: 'X17	BQ36 закрытие останова ',
        16: 'X20	Ручной режим останов ОТКРЫТЬ',
        17: 'X21	Ручной режим останов ЗАКРЫТЬ',
        18: 'X22	ЦЕПЬ',
        19: 'X23	Резерв',
        20: 'X24	NEW_BQ21 датчик конца звена',
        21: 'X25	NEW_BQ22 датчик начала звена',
        22: 'X26	Резерв',
        23: 'X27	Резерв',
        24: 'X30	останов ПОЗ-1 Закладка',
        25: 'X31	останов ПОЗ-3 Мост',
        26: 'X32	останов ПОЗ-5 Кардан',
        27: 'X33	останов ПОЗ-7 Переворот',
        28: 'X34	останов ПОЗ-10 Двигатель',
        29: 'X35	останов ПОЗ-14 Радиатор',
        30: 'X36	останов Натяжитель цепи (подвал)',
        31: 'X37	останов ПОЗ-26 Кабина',
        32: 'X40	останов ПОЗ-29 Надрамник',
        33: 'X41	останов ПОЗ-23 Заправка',
        34: 'X42	останов ПОЗ-32 Спуск'
    },
'RY': {  # Выходящие сигналы (Output Signals)
        0: 'Шток останова 1: Открыть',
        1: 'Шток останова 1: Закрыть',
        2: 'Шток останова 2: Открыть',
        3: 'Шток останова 2: Закрыть',
        5: 'Сигнал АВАРИИ из КОНТРОЛЛЕРА 0-активный'
    },
'RC': {  # Счётчики (Counters)
        30:  'время стоп закладка',
        31:  'время стоп мост',
        32:  'время стоп кардан',
        33:  'время стоп переварот',
        34:  'время стоп двигатель',
        35:  'время стоп радиатор',
        36:  'время стоп подвал',
        37:  'время стоп кабина',
        40:  'время стоп надрамник',
        41:  'время стоп заправка',
        42:  'время стоп спуск',
        44:  'время стоп от контроллера',
        45:  'время движения цепи',
        46:  'время стоп цепи',
        50:  'время работы контроллера',
        101: 'энерго независимый счетчик 1 X4 или X5',
        102: 'энерго независимый счетчик основной',
        103: 'энерго независимый счетчик 2 X24 + X25',
        104: 'энерго независимый счетчик при движении в -',
        110: 'энерго независимый счетчик машин'
    },
'RL': {  # Логическое оповещение (Logical Alerts)
        0:	'база  не выбрана',
        1:	'сработка датчика при остановленной цепи',
        2:	'Авария X24/X25',
        3:  'Необходимо выбрать расстаяние между передними стойками',
        4:	'Неопределенное состояние остановов',
        11:	'сигналы S9 и S10 не равны',
        12:	'сигналы S37 и S38 не равны',
        13:	'Ошибка!!! Нет передней тележки в останове (S9/S10)',
        14:	'Ручн. реж. Цепь STOP',
        16:	'Ошибка!!! Нет задней тележки в останове (S37/S38)',
        22:	'сигналы S21 и S22 не равны',
        23:	'сигналы S35 и S36 не равны',
        39:	'Предупреждение!!! Нет Задания!!! ',
        41:	'Переключитесь на работу от останова 2 Останов 1 не исправен!!!',
        42:	'Переключитесь на работу от останова 1 Останов 2 не исправен!!!',
        43:	'Авария датчиков S9,S10,S37,S38    Тележка в останове не определена!',
        44:	'S35 неисправен',
        45:	'S36 неисправен',
        50:	'Несоответствие  y0=1 s15=0',
        51:	'Несоответствие  y0=0 s15=1',
        52:	'Несоответствие  y1=1 s15=0',
        53:	'Несоответствие  y1=0 s15=1',
        54:	'Несоответствие  y2=1 s15=0',
        55:	'Несоответствие  y2=0 s15=1',
        56:	'Несоответствие  y3=1 s15=0',
        57:	'Несоответствие  y3=0 s15=1',
    },
'RD': {  # Регистры данных //
0:      'Сообщение "?" ',
1:      'Установка числа звеньев c пульта',
2:      'D2:=D714+7;',
3:      'D3:=D714+1;',
4:      'D4:=C102;',   
5:      'D5.0 запись на sd карту флаг Err/ok',
6:      'D6.0 чтение с sd карты флаг Err/ok',
9:      'Экран рецептов часть новая запись поле база',
10:     'Экран рецептов часть выбрано поле база',
20:     'Сообщения на экран 16 слов 1-ое',
21:     'Сообщения на экран 16 слов 2-ое',
22:     'Сообщения на экран 16 слов 3-ое',
23:     'Сообщения на экран 16 слов 4-ое',
24:     'Сообщения на экран 16 слов 5-ое',
25:     'Сообщения на экран 16 слов 6-ое',
26:     'Сообщения на экран 16 слов 7-ое',
27:     'Сообщения на экран 16 слов 8-ое',
28:     'Сообщения на экран 16 слов 9-ое',
29:     'Сообщения на экран 16 слов 10-ое',
30:     'Сообщения на экран 16 слов 11-ое',
31:     'Сообщения на экран 16 слов 12-ое',
32:     'Сообщения на экран 16 слов 13-ое',
33:     'Сообщения на экран 16 слов 14-ое',
34:     'Сообщения на экран 16 слов 15-ое',
35:     'Сообщения на экран 16 слов 16-ое',
40:     'Сообщения. Соответствует записям в GT Designer',
100:    'зажечь надпись 1-36 2-41 3-требование выбрать 36/41',
110:    'Переменная для хранения числа звеньев задней тележки',
120:    '"Текущее значение счетчика звеньев',
130:    'тек.знач.(1-4) режим D130:=C103;',
140:    'тек.знач.5-ый режим D140:=C101;',
200:    'коментарий закладчика формирующаяся база',
201:    'коментарий закладчика главный экран закладка 1',
202:    'коментарий закладчика главный экран закладка 2',
203:    'коментарий закладчика главный экран закладка 3',
204:    'коментарий закладчика главный экран закладка 4',
205:    'коментарий закладчика главный экран закладка 5',
206:    'коментарий закладчика главный экран закладка 6',
207:    'коментарий закладчика главный экран закладка 7',
208:    'коментарий закладчика главный экран закладка 8',
209:    'коментарий закладчика главный экран закладка 9',
210:    'коментарий закладчика главный экран закладка 10',
211:    'коментарий закладчика главный экран закладка 11',
300:    'Для ручного ввода списка с панели подвала 1 строка база -51-',
303:    'Для ручного ввода списка с панели подвала 2 строка база -29-',
306:    'Для ручного ввода списка с панели подвала 3 строка база -37-',
309:    'Для ручного ввода списка с панели подвала 4 строка база -35-',
312:    'Для ручного ввода списка с панели подвала 5 строка база -65-',
315:    'Для ручного ввода списка с панели подвала 6 строка база -66-',
318:    'Для ручного ввода списка с панели подвала 7 строка база -03-',
321:    'Для ручного ввода списка с панели подвала 8 строка база -18-',
324:    'Для ручного ввода списка с панели подвала 9 строка база -20-',
327:    'Для ручного ввода списка с панели подвала 10 строка база -21-',
330:    'Для ручного ввода списка с панели подвала 11 строка база -22-',
400:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D400:=11',
401:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D401:=12',
402:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D402:=13',
403:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D403:=14',
404:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D404:=15',
405:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D405:=16',
406:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D406:=17',
407:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D407:=18',
408:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D408:=20',
409:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D409:=21',
410:    'УСТАНОВИТЬ ЧИСЛО ЗВЕНЬЕВ БАЗ D410:=22',
430:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D430:=D400*320',
431:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D431:=D401*320',
432:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D432:=D402*320',
433:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D433:=D403*320',
434:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D434:=D404*320',
435:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D435:=D405*320',
436:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D436:=D406*320',
437:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D437:=D407*320',
438:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D438:=D408*320',
439:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D439:=D409*320',
440:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D440:=D410*320',
441:    'УМНОЖИТЬ НА 320 ДЛЯ КОЛОНКИ ДЛИНА D441:=D411*320',
500:    'добавить к модели данные базы',
501:    'добавить к модели данные базы',
502:    'добавить к модели данные базы',
505:    'по включению питания определяем это новый день?',
600:    'добавить к модели данные базы ',
700:    'Формирующеяся модель машины',
701:    'Формирующеяся модель машины',
702:    'Формирующеяся модель машины',
703:    'Формирующеяся модель машины',
704:    'Формирующеяся модель машины',
705:    'Формирующеяся модель машины',
706:    'Формирующеяся модель машины',
707:    'Формирующеяся модель машины',
708:    'Формирующеяся модель машины',
709:    'Формирующеяся модель машины',
710:    'Формирующеяся модель машины',
711:    'Формирующеяся название базы',
712:    'Формирующеяся название базы',
713:    'Формирующеяся название базы',
714:    'Формирующеяся количество звеньев для открытия останова',
715:    'Формирующеяся модель длинна',
990:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
991:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
992:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
993:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
994:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
995:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
996:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
997:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
998:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
999:    'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ',
1000:   'Режим работы счетчика int',
1001:   'Выбраный режим работы счетчика(ов) text',
1002:   'Выбраный режим работы счетчика(ов) text',
1003:   'Выбраный режим работы счетчика(ов) text',
1004:   'Выбраный режим работы счетчика(ов) text',
1005:   'Выбраный режим работы счетчика(ов) text',
1010:   'Вывод сообщения на экран при откате',
1030:   'отслеживание изменения D1030 для исключения ошибочного ввода числа звеньев при манипуляциях c архивом',
2000:   '1-ая строка задания модель',
2011:   '1-ая строка задания база',
2014:   '1-ая строка задания число звеньев',
2015:   '1-ая строка задания число длина',
2016:   '2-ая строка задания модель',
2027:   '2-ая строка задания база',
2030:   '2-ая строка задания число звеньев',
2031:   '2-ая строка задания число длина',
3000:   'содержит номер строки для записи задания основная',
3001:   'содержит номер строки для записи задания доп. из экрана схема',
3020:   '3020...3028 данные с рецептов',
3030:   'кол-во звеньев из рецепта',
4000:   'количество звеньев для перезапуска  счета',
5000:   'дребезг х24/х25',
    },
'RM': {  # Регистры промежуточных состояний (1 битные МАРКЕРА)
        0:  'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ GTSOFT',
        1:  'для сброса m99 и m101',
        2:  'кнопка установить "Да" на пульту',
        3:  'очистка данных задания',
        4:  'цепь не движется',
        5:  'Кнопка на пульту откл/вкл слежение(была)',
        6:  'кнопка на пульту квитирование (игнарирование) датчика закрытия',
        7:  'Сработает после ввода данных на понели d9',
        8:  'Сработает после ввода данных на понели d3030',
        9:  'тригер для экрана ЛОГ',
        10: 'откат сброс',
        11: 'Кнопка на пульту выбор остановов',
        12: 'Кнопка 1-Ручной 0-Авто',
        13: 'для истории ',
        14: 'кнопка на экране "Изменить число звеньев"',
        15: 'обновить данные на экране',
        16: 'сообщение на экран "задняя база сформирована"',
        17: 'для пуска цепи из ручного режима',
        18: 'для очистки сообщения цепь остановлена',
        19: 'выбран останов в ручном режиме 1/2',
        20: 'У0 в автомате',
        21: 'У1 в автомате',
        22: 'У2 в автомате',
        23: 'У3 в автомате',
        24: 'для скрытия и отоброжения кнопки"СЧИТАТЬ С АРХИВА"',
        25: 'У0 в ручном',
        26: 'У1 в ручном',
        27: 'У2 в ручном',
        28: 'У3 в ручном',
        29: '0-надпись ПЕРЕДНЯЯ 1-надпись ЗАДНЯЯ 4 позиция',
        30: '1-отобразить 0-крыть 4 позиция',
        31: '0-надпись ПЕРЕДНЯЯ 1-надпись ЗАДНЯЯ 3 позиция',
        32: '1-отобразить 0-крыть 3 позиция',
        33: '0-надпись ПЕРЕДНЯЯ 1-надпись ЗАДНЯЯ 2 позиция',
        34: '1-отобразить 0-крыть 2 позиция',
        35: '0-надпись ПЕРЕДНЯЯ 1-надпись ЗАДНЯЯ 1 позиция',
        36: '1-отобразить 0-крыть 1 позиция',
        37: 'Движение тележки 4 задняя',
        38: 'Движение тележки 3 задняя',
        39: 'Движение тележки 2',
        40: 'ГДЕ-ТО АВАРИЯ',
        41: 'Движение тележки 1',
        42: 'Движение тележки 4 передняя',
        43: 'Движение тележки 3 передняя',
        44: 'стереть d700',
        45: 'стоп/пуск узла контроля остановов',
        46: 'стоп/пуск узла счета баз',
        50: 'Не соответствие выход = вход в течении 2 сек',
        51: 'Исключить зацикливание',
        52: 'M52=0 Cообщение Необходимо выбрать расстаяние между передними стойками',
        53: 'M53:=1; надписи 36 и 41 моргают',
        97: 'УДАЛЕНИЕ СТРОЧКИ СО СМЕЩЕНИЕМ экран рецептов',
        99: 'начало передняя база',
        100:'сдвиг',
        101:'начало задняя база',
        102:'Сместить задание из списка на 1 позицию',
        105:'ОТ ЗАЦИКЛИВАНИЯ при записи архива не cd карту',
        109:'S37 задняя тележка в останове',
        110:'S38 задняя тележка в останове',
        111:'Тригер для формирования истории баз',
        115:'S15 останов 1 открыт',
        116:'S16 останов 1 закрыт',
        117:'S17 останов 2 открыт',
        118:'S18 останов 2 закрыт',
        121:'S21 счетчик звеньев',
        122:'S22 счетчик звеньев',
        123:'Счетчик с учетом дребезга',
        124:'Счетчик с учетом дребезга',
        125:'Счетчик с учетом дребезга',
        135:'S35 БВК на закрытие останова',
        136:'S36 БВК на закрытие останова',
        137:'S9 пернедняя тележка в останове',
        138:'S10 пернедняя тележка в останове',
        199:'Кнопка сброса ошибок на пульту',
        200:'Неверное положение останова',
        201:'Нет передней тележки',
        202:'число звеньев 33 а передней базы нет',
        203:'число звеньев = задней базе а ее нет',
        204:'выключить цепь',
        205:'нет задней тележки',
        300:'переключатель выбор схемы контр/реле',
        401:'добавить к модели данные базы',
        402:'ЗАНЕСЕНИЯ С АРХИВА В СТРОЧКУ задания экрае рецепты',
        403:'Если =1 то данные закладки из вне пересчет длинны и название базы',       
        500:'память Y0 открыть',
        501:'память Y1 закрыть',
        502:'память Y2 открыть',
        503:'память Y3 закрыть',
        519:'Выбранный останов 1-1 0-2',
        524:'Выбранный датчик щетчика звеньев с панели ',
        525:'Выбранный датчик задняя тележка с панели ',
        526:'Выбранный датчик передняя тележка с панели ',
        527:'Выбранный датчик на закрытие останова с панели',
    }
}

# Глобальная структура для хранения пользователей
users = {}  # {ip: {"hostname": "...", "last_seen": datetime, "user_agent": "...", "count": N}}
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
# --- НАСТРОЙКИ ПЛК--
SD_OFFSET = 20480 #holding
DEVICES = [{"id": "PLC_01", "ip": "192.168.161.1", "port": 502}]
RD_RANGES = [    (0,   100),  ]
RSD_RANGES = [
    (SD_OFFSET + 0,   1),   # SD0
    (SD_OFFSET + 200, 4),   # SD200..SD203
    (SD_OFFSET + 600, 13),  # SD600..SD612

    (SD_OFFSET + 210, 7),   # SD210..SD216 (часы)
    (SD_OFFSET + 519, 1),   # SD519
    (SD_OFFSET + 523, 3),   # SD523..SD525
]
# ================== НАСТРОЙКИ ==================
SERVER = r'192.168.149.238\SQLEXPRESS'
DATABASE = 'yamid'
USERNAME = 'klient'
PASSWORD = '1234567'
DRIVER = 'ODBC Driver 17 for SQL Server'
clients = {dev["id"]: ModbusTcpClient(dev["ip"], port=dev["port"], timeout=2) for dev in DEVICES}
conn_params = quote_plus(
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD}"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}")
def read_range(port, device, start, size):
    mc = Type3E()
    mc.connect(IP, port)
    try:
        return mc.batchread_wordunits(f"{device}{start}", size)
    finally:
        mc.close()

def parallel_read(device, total_words):
    tasks = []
    with ThreadPoolExecutor(max_workers=len(PORTS)) as ex:
        port_index = 0
        for i in range(0, total_words, CHUNK_SIZE):
            size = min(CHUNK_SIZE, total_words - i)
            port = PORTS[port_index]
            port_index = (port_index + 1) % len(PORTS)

            tasks.append(ex.submit(read_range, port, device, i, size))

        result = []
        for t in tasks:
            try:
                chunk = t.result()
                result.extend([v & 0xFFFF for v in chunk])
            except Exception as e:
                print(f"Ошибка в потоке при чтении {device}: {e}")
                raise

        return result
def red_slmp_fast():
    try:
        start = time.perf_counter()

        # Параллельное чтение больших областей
        all_d = parallel_read("D", TOTAL_D)
        all_r = parallel_read("R", TOTAL_R)

        # Однопоточные мелкие чтения (не критично)
        mc = Type3E()
        mc.connect(IP, 5000)

        rx = mc.batchread_bitunits("X0", 43)
        ry = mc.batchread_bitunits("Y0", 6)
        rl = mc.batchread_bitunits("L0", 58)
        rm = mc.batchread_bitunits("M0", 528)
        rc = mc.batchread_wordunits("CN30", 111)

        SD_LIST = [0,200,201,203,600,604,606,607,608,609,610,611,612,210,211,212,213,214,215,216,519,523,524,525]
        max_sd = max(SD_LIST)
        sd_range = mc.batchread_wordunits("SD0", max_sd+1)
        rsd = {addr: sd_range[addr] & 0xFFFF for addr in SD_LIST}

        mc.close()

        duration = time.perf_counter() - start
        print(f"Параллельный опрос завершён за {duration:.4f} сек.")

        return {
            "D": all_d,
            "RD": all_r,
            "X": [bool(x) for x in rx],
            "Y": [bool(y) for y in ry],
            "L": [bool(l) for l in rl],
            "M": [bool(m) for m in rm],
            "C": rc,
            "SD": rsd
        }
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        mc.close()    
def red_slmp():
 
    SD_LIST = [0, 200, 201, 203, 600, 604, 606, 607, 608, 609, 610, 611, 612, 210, 211, 212, 213, 214, 215, 216, 519, 523, 524, 525]
    mc = Type3E() 
    mc.connect("192.168.161.1", 5000)

    all_data_d = []
    all_data_rd = []

    try:
        start_time = time.perf_counter()

        # Чтение D (теперь меньше итераций)
        for i in range(0, TOTAL_WORDS_D, CHUNK_SIZE):
            size = min(CHUNK_SIZE, TOTAL_WORDS_D - i)
            # Сразу переводим в unsigned при чтении, если нужно
            chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"D{i}", size)]
            all_data_d.extend(chunk)

        # Чтение R
        for i in range(0, TOTAL_WORDS_RD, CHUNK_SIZE):
            size = min(CHUNK_SIZE, TOTAL_WORDS_RD - i)
            chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"R{i}", size)]
            all_data_rd.extend(chunk)

        # Биты читаем пачками (можно тоже объединять, если адреса рядом)
        rx_bits = mc.batchread_bitunits("X0", 43)
        ry_bits = mc.batchread_bitunits("Y0", 6)
        rl_bits = mc.batchread_bitunits("L0", 58)
        rm_bits = mc.batchread_bitunits("M0", 528)
        rc_word = mc.batchread_wordunits("CN30", 111) 

        # SD чтение
        max_sd = max(SD_LIST)
        sd_range = mc.batchread_wordunits("SD0", max_sd + 1)
        rsd_values = {addr: (sd_range[addr] & 0xFFFF) for addr in SD_LIST}

        duration = time.perf_counter() - start_time

        # Печатаем только статистику (печать данных тормозит цикл)

        results = {
            "X": [bool(b) for b in rx_bits], 
            "Y": [bool(b) for b in ry_bits],
            "C": rc_word,
            "L": [bool(b) for b in rl_bits],
            "M": [bool(b) for b in rm_bits],
            "SD": rsd_values, # Используйте словарь с конкретными SD, а не весь срез sd_range
            "D": all_data_d,
            "RD": all_data_rd
        }


        # Печатаем статистику в консоль для контроля
        print(f"Опрос завершен за {duration:.4f} сек.")

        return results
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        mc.close()

# ==========================
#  SCADA JSON MAPPER
# ==========================

def make_tag(tag_type, addr, val):
    """Единый формат тега"""
    return {
        "type": tag_type,
        "addr": addr,
        "val": val,
        "name": TAGS.get(tag_type, {}).get(addr, "")
    }


# ---------- RX ----------
def map_rx(bits):
    limit = min(len(bits), 40)
    return [make_tag("RX", i, bits[i]) for i in range(limit)]


# ---------- RY ----------
def map_ry(bits):
    limit = min(len(bits), 6)
    return [make_tag("RY", i, bits[i]) for i in range(limit)]


# ---------- RC ----------
def map_rc(values):
    # PLC адреса 30..157 (128 регистров)
    limit = min(len(values), 128)
    return [make_tag("RC", 30 + i, values[i]) for i in range(limit)]


# ---------- RL ----------
def map_rl(values):
    limit = min(len(values), 64)
    return [make_tag("RL", i, values[i]) for i in range(limit)]


# ---------- RM ----------
def map_rm(bits):
    limit = min(len(bits), 600)
    return [make_tag("RM", i, bits[i]) for i in range(limit)]


# ---------- RD ----------
def map_rd(values):
    return [make_tag("RD", i, values[i]) for i in range(len(values))]

# ---------- R ----------
def map_frd(values):
    return [make_tag("FRD", i, values[i]) for i in range(len(values))]

# ---------- SD----------
def map_sd(values):
    # values здесь — это словарь {0: val, 200: val, ...}
    return [make_tag("SD", addr, val) for addr, val in values.items()]

# ---------- ЕДИНЫЙ SCADA JSON ----------
def build_scada_json(data):
    # Теги, которые нужно фильтровать (только с именами)
    filtered_parts = (
        map_rx(data[0]) +
        map_ry(data[1]) +
        map_rc(data[2]) +
        map_rl(data[3]) +
        map_rm(data[5]) +
        map_sd(data[6]) )
    
    # Оставляем только те, где name не пустое
    tags_with_names = [tag for tag in filtered_parts if tag["name"]]
    
    # Регистры D добавляем целиком без фильтрации
    d_registers = map_rd(data[4])
     # Регистры R добавляем целиком без фильтрации
    r_registers = map_frd(data[7])   
    return tags_with_names + d_registers + r_registers


# --- ОПРОС ПЛК ---
def read_m_registers(client, m_offset, count=600):
    """Чтение больших объемов M регистров"""
    all_values = []
    chunk_size = 125
    steps = (count + chunk_size - 1) // chunk_size
    
    try:
        for step in range(steps):
            current_start = m_offset + step * chunk_size
            end_address = min(current_start + chunk_size, m_offset + count)
            
            result = client.read_coils(current_start, end_address - current_start, unit=1)
            if result.isError():
                raise Exception(f"Ошибка чтения M регистров с адреса {current_start}: {result.message}")
                
            all_values.extend(result.bits[:end_address-current_start])
        
        return all_values
    except Exception as e:
        print(f"Ошибка чтения M регистров: {e}")
        return None

def read_blocks(client, blocks, unit=1):
    result = []
    for addr, count in blocks:
        res = client.read_holding_registers(addr, count, unit=unit)
        if res.isError() or not res.registers:
            raise Exception(f"Ошибка чтения {addr}")
        result.extend(res.registers)
    return result
def read_all_d_registers(client, total=8000, unit=1):
    chunk = 125
    result = []

    for offset in range(0, total, chunk):
        size = min(chunk, total - offset)
        rd = client.read_holding_registers(offset, size, unit=unit)

        if rd.isError():
            raise Exception(f"Ошибка чтения блока offset={offset}, size={size}")

        result.extend(rd.registers)

    return result

def poll_device(client_not_used, device_info):
    """
    Теперь мы игнорируем старый объект client, 
    так как red_slmp сама управляет соединениями через pymcprotocol.
    """
    try:
        # Вызываем вашу новую быструю функцию
        # Она возвращает словарь: {"X": [...], "Y": [...], "C": [...], ...}
        data = red_slmp() #где то 1.2 сек
        # data = red_slmp_fast() #где то 1.1 сек нет смысла оставлять такую сложность
        
        if not data:
            return None

        # Формируем список в том же порядке, в котором его ждет build_scada_json:
        # [rx.bits, ry.bits, rc.registers, rl.bits, rd, rm_bits, rsd]
        return [
            data["X"],  
            data["Y"],  
            data["C"], 
            data["L"],  
            data["D"], 
            data["M"],  
            data["SD"], 
            data["RD"]
        ]
       
    except Exception as e:
        print(f"Ошибка высокоскоростного опроса ПЛК: {e}")
        return None


def get_plc_data():
    results = {}
    for dev_id, client in clients.items():
        data = poll_device(client, {"id": dev_id})
        if data: results[dev_id] = {"tags": build_scada_json(data)}
        else:    results[dev_id] = None
    return results
def resolve(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return "—"          # или "неизвестно", или просто пусто ""
    
# ================== УТИЛИТА ДЛЯ БЕЗОПАСНОГО SQL ==================
def safe_query(query):
    try:
        df = pd.read_sql(query, engine)
        return df
    except:
        return pd.DataFrame()

# ================== ЗАГРУЗКА ДАННЫХ (без print) ==================
def load_data_1(date_str):
    query = f"""
        SELECT Id, _data,
               cep_inf, inf1_, inf2_, inf3_, inf4_, inf5_,
               inf6_, inf7_, inf8_, inf9_, inf10_, inf11_, inf12_
        FROM dbo.graf_{date_str}
    """
    return safe_query(query).fillna("").to_dict(orient='records')

def load_data_2(date_str):
    query = f"""
        SELECT Id, na_lente, tek_stop
        FROM dbo.ist_ostan_{date_str}
    """
    return safe_query(query).fillna("").to_dict(orient='records')

def load_data_3(date_str):
    query = f"""
        SELECT Id, _data, cep_inf,
               inf1_, inf2_, inf3_, inf4_, inf5_,
               inf6_, inf7_, inf8_, inf9_, inf10_, inf11_, inf12_
        FROM dbo.data_on_line{date_str}
    """
    return safe_query(query).fillna("").to_dict(orient='records')

# ================== FLASK ==================
app = Flask(__name__)

@app.before_request
def log_user_to_console_and_memory():
    if request.path in ('/favicon.ico', '/static/', '/users'):
        return

    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if client_ip:
        client_ip = client_ip.split(',')[0].strip()

    hostname = resolve(client_ip)
    now = datetime.now()
    ua = request.headers.get('User-Agent', '—')[:80]

    # Обновляем/добавляем пользователя
    if client_ip not in users:
        users[client_ip] = {
            "hostname": hostname,
            "first_seen": now,
            "last_seen": now,
            "user_agent": ua,
            "request_count": 1
        }
    else:
        users[client_ip]["last_seen"] = now
        users[client_ip]["request_count"] += 1
        # Можно обновить UA, если изменился, но обычно не нужно

    # Вывод в консоль (как было)
    print(f"{now.strftime('%H:%M:%S')}   {client_ip:>15}   {hostname}")

# Роуты без изменений
@app.route("/")
def index():
    return render_template("index.html")
@app.route("/plc")
def plc_page():
    return render_template("plc.html")
@app.route("/data")
def get_data():
    date_str = request.args.get("date", "")
    if not date_str:
        return jsonify({"graf_": [], "list_ostan": [], "data_on_line": []})

    graf = load_data_1(date_str)
    ostan = load_data_2(date_str)
    online = load_data_3(date_str)

    return jsonify({
        "graf_": graf,
        "list_ostan": ostan,
        "data_on_line": online
    })
@app.route("/plc_get")
def plc_get():
    # Здесь вызывается тяжелый опрос
    plc_data = get_plc_data()
    return jsonify(plc_data)

@app.route("/plc_XYL")
def plc_XYL():
    mc = Type3E()
    try:
        mc.connect("192.168.161.1", 5000)

        rx_bits = mc.batchread_bitunits("X0", 43)
        ry_bits = mc.batchread_bitunits("Y0", 6)
        rl_bits = mc.batchread_bitunits("L0", 58)
        c102_val = mc.batchread_wordunits("CN102", 1)[0]
        d4000_val = mc.batchread_wordunits("D4000", 1)[0]

        result = []

        # X
        for i, val in enumerate(rx_bits):
            addr_oct = format(i, "o")      # восьмеричный
            addr_dec = i                   # десятичный
            result.append({
                "type": "X",
                "addr_oct": addr_oct,
                "addr_dec": addr_dec,
                "val": val
            })

        # Y
        for i, val in enumerate(ry_bits):
            addr_oct = format(i, "o")
            addr_dec = i
            result.append({
                "type": "Y",
                "addr_oct": addr_oct,
                "addr_dec": addr_dec,
                "val": val
            })
        # L
        for i, val in enumerate(rl_bits):
            addr_oct = format(i, "o")
            addr_dec = i
            result.append({
                "type": "L",
                "addr_oct": addr_oct,
                "addr_dec": addr_dec,
                "val": val
            })
        # C и D
        result.append({"type": "C", "addr_dec": 102, "val": c102_val})
        result.append({"type": "D", "addr_dec": 4000, "val": d4000_val})

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        mc.close()

    
# Новый роут — список пользователей (JSON для фронта)
@app.route("/users")
def get_users():
    now = datetime.now()
    active_threshold = now - timedelta(minutes=30)  # считаем активным, если был запрос за последние 30 мин

    active_users = []
    for ip, data in users.items():
        if data["last_seen"] >= active_threshold:
            active_users.append({
                "ip": ip,
                "hostname": data["hostname"],
                "last_seen": data["last_seen"].strftime("%H:%M:%S"),
                "requests": data["request_count"],
                "user_agent": data["user_agent"][:60]  # укорачиваем, чтобы не ломать таблицу
            })

    # Сортируем по времени последнего запроса (новые сверху)
    active_users.sort(key=lambda x: x["last_seen"], reverse=True)

    return jsonify({"users": active_users})
@app.after_request
def disable_cache(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

def open_browser():
    webbrowser.open("http://127.0.0.1")

if __name__ == "__main__":
    threading.Timer(1.0, open_browser).start()
    app.run(
        host="0.0.0.0",
        port=80,
        debug=True,
        use_reloader=False   # ← вот это ключевое
    )
