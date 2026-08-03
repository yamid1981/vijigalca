from flask import Flask, render_template, jsonify, request, send_file, after_this_request
import json
import os
import glob
import pyodbc
from datetime import datetime
import re
import configparser
import socket
from pymcprotocol import Type3E
import logging
import threading
import time
from collections import deque
from typing import Optional, Dict, List
import gzip
import traceback
import shutil
import tempfile




app = Flask(__name__)

# =========================== НАСТРОЙКИ ==========================
PLC_DATA_CACHE = None
PLC_CACHE_TIME = 0
CACHE_TTL = 1.0  # Секунды
JSON_FILE = 'plc_state.json'
# Путь должен совпадать с TIMELINE_LOG_BASE из твоего скрипта
TIMELINE_LOG_DIR = r"c:/log" 
LOG_FILE_PREFIX = "timeline_log_"
# Параметры подключения к БД
DB_SERVER = r'tcp:127.0.0.1\OITNK,1433'
DB_NAME = 'yamid'
DB_USER = 'klient'
DB_PASS = '1234567'
DB_DRIVER = '{ODBC Driver 17 for SQL Server}'

SQL_CONN_STR = (
    f"Driver={DB_DRIVER};Server={DB_SERVER};Database={DB_NAME};"
    f"UID={DB_USER};PWD={DB_PASS};"
)

# PLC Settings
PLC_IP = "192.168.161.1"
PLC_PORTS = [5000, 5001, 5002, 5003, 5004]
PLC_TIMEOUT = 5
POLL_INTERVAL = 0.3

# Event Log Settings
EVENT_LOG_PATH = r"c:/log/event_log.jsonl"
EVENT_STATE_PATH = r"c:/log/event_state.json"  # Файл для сохранения состояния буфера
EVENT_POLL_INTERVAL = 0.5
CHUNK_SIZE = 450
TOTAL_WORDS_D = 8000
TOTAL_WORDS_RD = 31200
LOG_RETENTION_DAYS = 10
EVENT_STATE_SAVE_INTERVAL = 100  # Сохранять состояние каждые N событий

# TAGS Dictionary from old app.py
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
        7:  'X7	    BQ38 Задняя тележка перед остановом ',
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
        41: 'Движение тележки 4 передняя',
        42: 'Движение тележки 3 передняя',
        44: 'стереть d700',
        45: 'стоп/пуск узла контроля остановов',
        46: 'стоп/пуск узла счета баз',
        50: 'Не соответствие выход = вход в течении 2 сек',
        51: 'Исключить зацикливание',
        52: 'M52=0 Сoобщение Необходимо выбрать расстаяние между передними стойками',
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

# Глобальные переменные для событий
EVENT_LOG = deque(maxlen=50000)  # Увеличено с 1000 для хранения событий за весь день
EVENT_LOCK = threading.Lock()
EVENT_INITIALIZED = False
EVENT_PREV = {
    "RX": {},
    "RY": {},
    "RL": {}
}
EVENT_STATE_COUNTER = 0  # Счётчик событий для периодического сохранения
PLC_LOCK = threading.Lock()

# =========================== USER TRACKING ==========================
# Словарь для отслеживания пользователей (IP -> info)
USERS = {}
USERS_LOCK = threading.Lock()

def resolve(ip):
    """Получает hostname по IP адресу"""
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return "—"

@app.before_request
def track_user():
    """Отслеживает пользователей, обращающихся к сайту"""
    # Исключаем статику и служебные пути
    if request.path in ('/favicon.ico',) or request.path.startswith('/static/'):
        return
    
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if client_ip:
        client_ip = client_ip.split(',')[0].strip()
    
    hostname = resolve(client_ip)
    now = datetime.now()
    ua = request.headers.get('User-Agent', '—')[:80]
    
    with USERS_LOCK:
        if client_ip not in USERS:
            USERS[client_ip] = {
                "ip": client_ip,
                "hostname": hostname,
                "first_seen": now,
                "last_seen": now,
                "user_agent": ua,
                "request_count": 1
            }
        else:
            USERS[client_ip]["last_seen"] = now
            USERS[client_ip]["request_count"] += 1
            # Можно обновить UA, если изменился
            USERS[client_ip]["user_agent"] = ua
    
    # Вывод в консоль (как было в app.py)
    print(f"{now.strftime('%H:%M:%S')}   {client_ip:>15}   {hostname}")

def get_ist_ostan_table_name() -> str:
    """Возвращает имя таблицы истории остановов на сегодня"""
    return "ist_ostan_" + datetime.now().strftime("%Y_%m_%d")

def get_graf_table_name() -> str:
    """Возвращает имя таблицы графика на сегодня"""
    return "graf_" + datetime.now().strftime("%Y_%m_%d")

# Маппинг зон на реальные колонки в БД
ZONE_TO_DB_COLUMN = {
    'cep_inf': 'cep_inf',
    'inf1_': 'inf1_',
    'inf2_': 'inf2_',
    'inf3_': 'inf3_',
    'inf4_': 'inf4_',
    'inf5_': 'inf5_',
    'inf6_': 'inf6_',
    'inf7_': 'inf8_',   # inf7_ зона пишется в колонку inf8_
    'inf8_': 'inf9_',   # inf8_ зона пишется в колонку inf9_
    'inf9_': 'inf10_',  # inf9_ зона пишется в колонку inf10_
    'inf10_': 'inf11_', # inf10_ зона пишется в колонку inf11_
    'inf11_': 'inf7_',  # inf11_ зона пишется в колонку inf7_
    'inf12_': 'inf12_',
}

# =========================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========================
def calculate_duration(start_time, end_time):
    """Вычисляет длительность между двумя временными метками"""
    try:
        start = datetime.strptime(start_time, "%H:%M:%S")
        end = datetime.strptime(end_time, "%H:%M:%S")
        diff = end - start
        total_seconds = int(diff.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    except:
        return "0:00:00"

def get_archive_table_name(base_name: str, date_str: str) -> str:
    """Формирует имя таблицы для архива"""
    if not date_str or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        raise ValueError("Неверный формат даты")
    
    date_suffix = date_str.replace("-", "_")
    
    if base_name == "data_on_line":
        return f"{base_name}{date_suffix}"
    else:
        return f"{base_name}_{date_suffix}"

def get_table_suffix(date_str: str) -> str:
    """Безопасно формирует суффикс таблицы из даты YYYY-MM-DD в YYYY_MM_DD"""
    if not date_str or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        raise ValueError("Неверный формат даты")
    return "_" + date_str.replace("-", "_")

# =========================== EVENT LOG FUNCTIONS ==========================
def get_log_path(date=None):
    """Возвращает путь к лог-файлу за конкретную дату"""
    if date is None:
        date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    base, ext = os.path.splitext(EVENT_LOG_PATH)
    return f"{base}_{date_str}.json"

def cleanup_old_logs(keep_days=LOG_RETENTION_DAYS):
    """Удаляет лог-файлы старше keep_days дней"""
    try:
        today = datetime.now().date()
        base, ext = os.path.splitext(EVENT_LOG_PATH)
        log_dir = os.path.dirname(EVENT_LOG_PATH) or "."
        
        if not os.path.exists(log_dir):
            return []
            
        deleted = []
        pattern = os.path.join(log_dir, f"{os.path.basename(base)}_*.json")
        
        for filepath in glob.glob(pattern):
            try:
                filename = os.path.basename(filepath)
                date_part = filename.replace(f"{os.path.basename(base)}_", "").replace(".json", "")
                file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                
                if (today - file_date).days >= keep_days:
                    os.remove(filepath)
                    deleted.append(filename)
                    print(f"[{datetime.now()}] Deleted old log: {filename}")
            except (ValueError, Exception):
                continue
        
        return deleted
    except Exception as e:
        print(f"Cleanup error: {e}")
        return []

def write_event_to_file(event):
    """Записывает событие в файл текущего дня"""
    try:
        log_path = get_log_path()
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            f.flush()
    except Exception as e:
        print(f"Error writing event: {e}")

def append_event_to_disk(event):
    """Потокобезопасная буферизированная запись"""
    if not hasattr(append_event_to_disk, "buffer"):
        append_event_to_disk.buffer = []
        append_event_to_disk.last_flush = time.time()
    
    append_event_to_disk.buffer.append(event)
    
    if (time.time() - append_event_to_disk.last_flush > 5) or len(append_event_to_disk.buffer) >= 50:
        try:
            log_path = get_log_path()
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                for ev in append_event_to_disk.buffer:
                    f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            append_event_to_disk.buffer.clear()
            append_event_to_disk.last_flush = time.time()
        except Exception as e:
            print(f"Ошибка записи лога: {e}")

# =========================== EVENT STATE PERSISTENCE ==========================
def save_event_state():
    """Сохраняет состояние EVENT_PREV, EVENT_LOG и EVENT_INITIALIZED в файл"""
    global EVENT_STATE_COUNTER
    try:
        with EVENT_LOCK:
            state = {
                "EVENT_PREV": {
                    "RX": {str(k): v for k, v in EVENT_PREV.get("RX", {}).items()},
                    "RY": {str(k): v for k, v in EVENT_PREV.get("RY", {}).items()},
                    "RL": {str(k): v for k, v in EVENT_PREV.get("RL", {}).items()},
                    "CN102": EVENT_PREV.get("CN102"),
                    "D714": EVENT_PREV.get("D714"),
                    "D4000": EVENT_PREV.get("D4000"),
                },
                "EVENT_LOG": list(EVENT_LOG),  # deque конвертируем в список
                "EVENT_INITIALIZED": EVENT_INITIALIZED,
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "event_count": len(EVENT_LOG)
            }
        
        os.makedirs(os.path.dirname(EVENT_STATE_PATH) or ".", exist_ok=True)
        with open(EVENT_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Состояние событий сохранено: {len(EVENT_LOG)} событий, INITIALIZED={EVENT_INITIALIZED}")
    except Exception as e:
        print(f"Ошибка сохранения состояния событий: {e}")

def load_event_state():
    """Загружает состояние EVENT_PREV, EVENT_LOG и EVENT_INITIALIZED из файла"""
    global EVENT_LOG, EVENT_PREV, EVENT_INITIALIZED, EVENT_STATE_COUNTER
    try:
        if not os.path.exists(EVENT_STATE_PATH):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Файл состояния событий не найден: {EVENT_STATE_PATH}")
            return False
        
        with open(EVENT_STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        
        with EVENT_LOCK:
            # Восстанавливаем EVENT_PREV (ключи приводим к int)
            EVENT_PREV["RX"] = {int(k): v for k, v in state.get("EVENT_PREV", {}).get("RX", {}).items()}
            EVENT_PREV["RY"] = {int(k): v for k, v in state.get("EVENT_PREV", {}).get("RY", {}).items()}
            EVENT_PREV["RL"] = {int(k): v for k, v in state.get("EVENT_PREV", {}).get("RL", {}).items()}
            EVENT_PREV["CN102"] = state.get("EVENT_PREV", {}).get("CN102")
            EVENT_PREV["D714"] = state.get("EVENT_PREV", {}).get("D714")
            EVENT_PREV["D4000"] = state.get("EVENT_PREV", {}).get("D4000")
            
            # Восстанавливаем EVENT_LOG (с ограничением maxlen)
            loaded_events = state.get("EVENT_LOG", [])
            EVENT_LOG.clear()
            # Добавляем события, учитывая maxlen (deque сам отбросит старые если их много)
            for ev in loaded_events:
                EVENT_LOG.append(ev)
            
            EVENT_INITIALIZED = state.get("EVENT_INITIALIZED", False)
            EVENT_STATE_COUNTER = len(EVENT_LOG)
        
        saved_at = state.get("saved_at", "unknown")
        event_count = state.get("event_count", len(EVENT_LOG))
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Состояние событий загружено: {event_count} событий, сохранено {saved_at}, INITIALIZED={EVENT_INITIALIZED}")
        return True
    except Exception as e:
        print(f"Ошибка загрузки состояния событий: {e}")
        return False

def maybe_save_event_state():
    """Периодически сохраняет состояние событий"""
    global EVENT_STATE_COUNTER
    with EVENT_LOCK:
        EVENT_STATE_COUNTER += 1
        if EVENT_STATE_COUNTER >= EVENT_STATE_SAVE_INTERVAL:
            EVENT_STATE_COUNTER = 0
            # Сохраняем в отдельном потоке, чтобы не блокировать опрос
            threading.Thread(target=save_event_state, daemon=True).start()

def update_event_log_bits(rx_bits, ry_bits, rl_bits, c102, d714, d4000):
    global EVENT_INITIALIZED
    if rx_bits is None or ry_bits is None or rl_bits is None:
        print(f"Пропуск цикла: пустые данные X={len(rx_bits) if rx_bits is not None else None}, Y={len(ry_bits) if ry_bits is not None else None}, L={len(rl_bits) if rl_bits is not None else None}")
        return
    
    now_full = datetime.now()
    now = now_full.strftime("%H:%M:%S")
    
    with EVENT_LOCK:
        if not EVENT_INITIALIZED:
            if len(rx_bits) != 43 or len(ry_bits) != 6 or len(rl_bits) != 58:
                print(f"Ошибка инициализации: неожиданная длина X={len(rx_bits)}, Y={len(ry_bits)}, L={len(rl_bits)}")
                return
            
            EVENT_PREV["RX"] = {i: bool(v) for i, v in enumerate(rx_bits)}
            EVENT_PREV["RY"] = {i: bool(v) for i, v in enumerate(ry_bits)}
            EVENT_PREV["RL"] = {i: bool(v) for i, v in enumerate(rl_bits)}
            EVENT_PREV["CN102"] = c102
            EVENT_PREV["D714"] = d714
            EVENT_PREV["D4000"] = d4000
            EVENT_INITIALIZED = True
            print(f"Инициализация EVENT_PREV завершена: X={len(rx_bits)}, Y={len(ry_bits)}, L={len(rl_bits)}, CN102={c102}, D714={d714}, D4000={d4000}")
            return
        
        def process(bits, tag_type, prefix, names):
            for addr, val in enumerate(bits):
                current = bool(val)
                if addr not in EVENT_PREV[tag_type]:
                    EVENT_PREV[tag_type][addr] = current
                    continue
                
                prev = EVENT_PREV[tag_type][addr]
                if prev != current:
                    direction = "↑" if current else "↓"
                    event = {
                        "ts": now_full.isoformat(timespec="seconds"),
                        "time": now,
                        "tag": f"{prefix}{addr}",
                        "name": f"{direction} {names.get(addr, '')}".strip()
                    }
                    EVENT_LOG.appendleft(event)
                    append_event_to_disk(event)
                    EVENT_PREV[tag_type][addr] = current
        
        process(rx_bits, "RX", "X", TAGS.get("RX", {}))
        process(ry_bits, "RY", "Y", TAGS.get("RY", {}))
        process(rl_bits, "RL", "L", TAGS.get("RL", {}))
        
        # CN102
        prev_c102 = EVENT_PREV.get("CN102")
        if prev_c102 is None:
            EVENT_PREV["CN102"] = c102
        elif prev_c102 != c102:
            event = {
                "ts": now_full.isoformat(timespec="seconds"),
                "time": now,
                "tag": "CN102",
                "name": f"{prev_c102} → {c102}"
            }
            EVENT_LOG.appendleft(event)
            append_event_to_disk(event)
            EVENT_PREV["CN102"] = c102
            print(f"[{now}] CN102 изменился: {prev_c102} → {c102}")
        
        # Периодическое сохранение состояния
        maybe_save_event_state()
        
        print(f"[{now}] Опрос завершен, событий в буфере: {len(EVENT_LOG)}")

def event_poll_loop_from_json():
    """Читает активный JSON буфер (двойная буферизация)"""
    global EVENT_INITIALIZED, EVENT_PREV
    
    json_file_path = 'plc_state_A.json'  # Начинаем с A
    last_active_file = 'plc_state_A.json'
    consecutive_errors = 0
    
    while True:
        try:
            # Проверяем, какой файл активен
            if os.path.exists('plc_state.json'):
                try:
                    with open('plc_state.json', 'r', encoding='utf-8') as f:
                        meta = json.load(f)
                        json_file_path = meta.get('active', 'plc_state_A.json')
                except:
                    pass
            
            # Если файл не изменился, читаем тот же самый
            if json_file_path != last_active_file:
                last_active_file = json_file_path
                # Небольшая пауза чтобы файл точно записался
                time.sleep(0.01)
            
            if not os.path.exists(json_file_path):
                time.sleep(0.5)
                continue
                
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            rx_bits = data.get("X", [])
            ry_bits = data.get("Y", [])
            rl_bits = data.get("L", [])
            c102_val = data.get("C", [0]*111)[72] if len(data.get("C", [])) > 72 else 0
            d714_val = data.get("D714", 0)
            d4000_val = data.get("D4000", 0)
            
            if not rx_bits or not ry_bits or not rl_bits:
                time.sleep(0.5)
                continue

            update_event_log_bits(rx_bits, ry_bits, rl_bits, c102_val, d714_val, d4000_val)
            consecutive_errors = 0
            
        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors % 10 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка чтения JSON событий: {e}")
        
        time.sleep(EVENT_POLL_INTERVAL)

# =========================== SCADA JSON MAPPER ==========================
def make_tag(tag_type, addr, val):
    return {
        "type": tag_type,
        "addr": addr,
        "val": val,
        "name": TAGS.get(tag_type, {}).get(addr, "")
    }

def map_list(tag_type, values, limit=None, offset=0):
    if limit is None:
        limit = len(values)
    limit = min(len(values), limit)
    return [make_tag(tag_type, offset + i, values[i]) for i in range(limit)]

def map_sd(values):
    return [make_tag("SD", addr, val) for addr, val in values.items()]

def build_scada_json(data):
    filtered_parts = (
        map_list("RX", data[0], limit=40) +
        map_list("RY", data[1], limit=6) +
        map_list("RC", data[2], limit=128, offset=30) +
        map_list("RL", data[3], limit=64) +
        map_list("RM", data[5], limit=600) +
        map_sd(data[6])
    )
    
    tags_with_names = [tag for tag in filtered_parts if tag["name"]]
    d_registers = map_list("RD", data[4])
    r_registers = map_list("FRD", data[7])
    return tags_with_names + d_registers + r_registers

# =========================== ROUTES ==========================

@app.route('/')
def index():
    """Главная страница с интерфейсом index2.html"""
    return render_template('index2.html')

@app.route('/plc')
def plc_interface():
    """Старый интерфейс plc.html"""
    return render_template('plc.html')

@app.route('/plc_admin')
def plc_admin_interface():
    """PLC Diagnostic интерфейс plc_admin.html"""
    return render_template('plc_admin.html')

@app.route('/timeline')
def timeline_interface():
    """Timeline визуализация датчиков за день"""
    return render_template('timeline.html')

# Путь к timeline лог файлам (базовое имя)
TIMELINE_LOG_BASE = r"c:/log/timeline_log"

def get_timeline_log_path(date=None):
    """Возвращает путь к лог-файлу timeline за конкретную дату"""
    if date is None:
        date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    return f"{TIMELINE_LOG_BASE}_{date_str}.jsonl"

@app.route('/api/timeline_data')
def get_timeline_data():
    date_str = request.args.get('date')
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
        
    try:
        # 1. Формируем базовый путь (без расширения)
        base_log_path = get_timeline_log_path(datetime.strptime(date_str, "%Y-%m-%d"))
        
        # 2. Определяем, какой файл существует: приоритет у .gz, затем обычный .jsonl
        gz_path = base_log_path.replace(".jsonl", ".jsonl.gz")
        
        events = []
        
        if os.path.exists(gz_path):
            # Читаем сжатый архив (режим 'rt' — read text)
            with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try: events.append(json.loads(line))
                        except: pass
        elif os.path.exists(base_log_path):
            # Читаем обычный текстовый файл (актуально для сегодняшнего дня)
            with open(base_log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try: events.append(json.loads(line))
                        except: pass

        return jsonify({
            'date': date_str,
            'events': events,
            'total_events': len(events)
        })
    except Exception as e:
        print(f"Ошибка в get_timeline_data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/timeline_log')
def get_timeline_log():
    """Отдаёт timeline log с подробным логированием ошибок"""
    try:
        date_str = request.args.get('date') or datetime.now().strftime("%Y-%m-%d")
        is_today = (date_str == datetime.now().strftime("%Y-%m-%d"))
        
        gz_path = f"c:/log/timeline_log_{date_str}.jsonl.gz"
        
        print(f"[DEBUG] date_str={date_str}, is_today={is_today}")
        print(f"[DEBUG] gz_path={gz_path}")
        print(f"[DEBUG] exists={os.path.exists(gz_path)}")
        
        if not os.path.exists(gz_path):
            return jsonify({'date': date_str, 'events': [], 'total_events': 0})
        
        if is_today:
            # Делаем временную копию
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.jsonl.gz')
            os.close(tmp_fd)
            
            print(f"[DEBUG] tmp_path={tmp_path}")
            
            try:
                # Копируем файл
                shutil.copy2(gz_path, tmp_path)
                print(f"[DEBUG] Файл скопирован успешно")
                
                @after_this_request
                def cleanup(response):
                    def delete_later():
                        time.sleep(2)
                        try: os.remove(tmp_path)
                        except: pass
                    import threading
                    threading.Thread(target=delete_later, daemon=True).start()
                    return response
                
                response = send_file(
                    tmp_path,
                    mimetype='application/gzip',
                    download_name=f'timeline_log_{date_str}.jsonl.gz'
                )
                response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
                print(f"[DEBUG] Отправка ответа успешна")
                return response
                
            except Exception as e:
                print(f"[ERROR] Ошибка при копировании/отправке: {e}")
                traceback.print_exc()
                try:
                    os.remove(tmp_path)
                except:
                    pass
                return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500
        
        else:
            # Прошлые дни — читаем напрямую
            response = send_file(
                gz_path, 
                mimetype='application/gzip',
                download_name=f'timeline_log_{date_str}.jsonl.gz'
            )
            response.headers['Cache-Control'] = 'public, max-age=86400'
            return response
            
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        traceback.print_exc()
        return jsonify({
            'error': str(e), 
            'traceback': traceback.format_exc()
        }), 500

def get_active_plc_state():
    """Безопасно читает данные из активного JSON-буфера"""
    try:
        active_file = 'plc_state_A.json' # Значение по умолчанию
        
        # Пытаемся узнать, какой файл сейчас активен
        if os.path.exists('plc_state.json'):
            with open('plc_state.json', 'r', encoding='utf-8') as f:
                meta = json.load(f)
                active_file = meta.get('active', 'plc_state_A.json')
        
        # Читаем сам файл с данными
        if os.path.exists(active_file):
            with open(active_file, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ Ошибка чтения активного состояния ПЛК: {e}")
    
    return None
@app.route('/api/state')
def get_state():
    """API endpoint для получения текущего состояния из активного JSON буфера"""
    data = get_active_plc_state()
    
    if data:
        return jsonify(data)
    else:
        return jsonify({'error': 'Файл состояния не найден или пуст. Подождите 1-2 секунды после запуска логгера.'}), 404

@app.route('/api/released')
def get_released():
    """API endpoint для получения списка выпущенных машин из БД"""
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        ist_table_name = get_ist_ostan_table_name()
        
        query = f"""
            SELECT na_lente, tek_stop 
            FROM {ist_table_name} 
            WHERE na_lente IS NOT NULL AND DATALENGTH(na_lente) > 0
            ORDER BY Id
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        released_list = []
        for row in rows:
            na_lente_str = str(row.na_lente).strip() if row.na_lente else ''
            
            if na_lente_str:
                tek_stop_str = str(row.tek_stop).strip() if row.tek_stop else ''
                released_list.append({
                    'na_lente': na_lente_str,
                    'tek_stop': tek_stop_str
                })
        
        return jsonify(released_list)
    
    except Exception as e:
        print(f" Ошибка в get_released: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/<zone_name>')
def get_zone_history(zone_name):
    """Получение истории остановов для конкретной зоны (с группировкой)"""
    try:
        db_column = ZONE_TO_DB_COLUMN.get(zone_name, zone_name)
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        graf_table_name = get_graf_table_name()
        
        query = f"""
            SELECT _data, {db_column}
            FROM {graf_table_name}
            WHERE {db_column} IS NOT NULL
            ORDER BY Id
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        stops = []
        current_stop_start = None
        
        for row in rows:
            time_str = str(row._data).strip() if row._data else None
            value = int(row.__getitem__(1)) if row.__getitem__(1) is not None else 0
            
            if value == 1 and current_stop_start is None:
                current_stop_start = time_str
            elif value == 0 and current_stop_start is not None:
                stops.append({
                    'start': current_stop_start,
                    'end': time_str,
                    'duration': calculate_duration(current_stop_start, time_str)
                })
                current_stop_start = None
        
        if current_stop_start is not None:
            current_time = datetime.now().strftime("%H:%M:%S")
            stops.append({
                'start': current_stop_start,
                'end': 'сейчас',
                'duration': calculate_duration(current_stop_start, current_time)
            })
        
        total_stops_count = len(stops)
        stops.reverse()
        
        return jsonify({
            'total_stops': total_stops_count,
            'stops': stops
        })
    
    except Exception as e:
        print(f"Ошибка в get_zone_history: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/graf_data')
def get_graf_data():
    """Получение всех данных из graf таблицы за сегодня"""
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        graf_table_name = get_graf_table_name()
        
        db_columns = [ZONE_TO_DB_COLUMN[zone] for zone in ZONE_TO_DB_COLUMN.keys()]
        query = f"""
            SELECT Id, _data, {', '.join(db_columns)}
            FROM {graf_table_name}
            ORDER BY Id
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        zone_names = list(ZONE_TO_DB_COLUMN.keys())
        
        data = []
        for row in rows:
            record = {'Id': row[0]}
            record['_data'] = str(row[1]).strip() if row[1] else None
            
            for i, zone in enumerate(zone_names):
                val = row[i + 2]
                record[zone] = int(val) if val is not None else 0
            
            data.append(record)
        
        return jsonify(data)
    
    except Exception as e:
        print(f"Ошибка в get_graf_data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/user_logins')
def get_user_logins():
    """Получение списка пользователей из таблицы UserLogins"""
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        query = """
            SELECT TOP (1000) [LogID], [LogTime], [SystemUser], [AppLoginName], [HostName], [IPAddress], [AppName]
            FROM [yamid].[dbo].[UserLogins]
            ORDER BY [LogTime] DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logins_list = []
        for row in rows:
            logins_list.append({
                'LogID': row.LogID,
                'LogTime': row.LogTime.strftime("%Y-%m-%d %H:%M:%S") if row.LogTime else '',
                'SystemUser': row.SystemUser,
                'AppLoginName': row.AppLoginName,
                'HostName': row.HostName,
                'IPAddress': row.IPAddress,
                'AppName': row.AppName
            })
        
        return jsonify(logins_list)
    
    except Exception as e:
        print(f"Ошибка в get_user_logins: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/users')
def get_users():
    """Получение списка пользователей, зашедших на сайт (в памяти)"""
    try:
        with USERS_LOCK:
            users_list = []
            now = datetime.now()
            for ip, info in USERS.items():
                # Считаем активными тех, кто был активен менее 30 минут назад
                if (now - info["last_seen"]).total_seconds() < 1800:
                    users_list.append({
                        "ip": info["ip"],
                        "hostname": info["hostname"],
                        "first_seen": info["first_seen"].strftime("%H:%M:%S"),
                        "last_seen": info["last_seen"].strftime("%H:%M:%S"),
                        "requests": info["request_count"],
                        "user_agent": info["user_agent"]
                    })
            
            # Сортируем по последней активности (новые сверху)
            users_list.sort(key=lambda x: x["last_seen"], reverse=True)
        
        return jsonify({"users": users_list})
    
    except Exception as e:
        print(f"Ошибка в get_users: {e}")
        return jsonify({"error": str(e)}), 500

# ========================== АРХИВНЫЕ ENDPOINTS ==========================

@app.route('/api/archive/state')
def get_archive_state():
    """Получает сводные данные из data_on_line за выбранную дату"""
    date_str = request.args.get('date')
    try:
        table_name = get_archive_table_name("data_on_line", date_str)
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        if cursor.fetchone()[0] == 0:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Таблица {table_name} не найдена в базе данных.'}), 404
        
        cursor.execute(f"""
            SELECT Id, cep_inf, inf1_, inf2_, inf3_, inf4_, inf5_, inf6_, inf7_, inf8_, inf9_, inf10_, inf11_, inf12_ 
            FROM {table_name} WHERE Id IN (1, 2)
        """)
        rows = cursor.fetchall()
        
        cursor.execute(f"SELECT _data, inf1_, inf2_ FROM {table_name} WHERE Id = 3")
        row3 = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        zones_data = []
        zone_keys = ['cep_inf', 'inf1_', 'inf2_', 'inf3_', 'inf4_', 'inf5_', 'inf6_', 'inf7_', 'inf8_', 'inf9_', 'inf10_', 'inf11_', 'inf12_']
        
        row1_dict = {r.Id: r for r in rows}.get(1)
        row2_dict = {r.Id: r for r in rows}.get(2)
        
        for z_key in zone_keys:
            current_t = getattr(row1_dict, z_key, "0:00:00") if row1_dict else "0:00:00"
            total_t = getattr(row2_dict, z_key, "0:00:00") if row2_dict else "0:00:00"
            
            zones_data.append({
                "name": z_key,
                "active": False,
                "current_time": str(current_t) if current_t else "0:00:00",
                "total_time": str(total_t) if total_t else "0:00:00",
                "stop_count": 0,
                "c_value": 0 
            })
            
        c110_val = str(row3._data).strip() if row3 and row3._data else "0"
        c50_val = str(row3.inf1_).strip() if row3 and row3.inf1_ else "0:00:00"
        c45_val = str(row3.inf2_).strip() if row3 and row3.inf2_ else "0:00:00"
            
        return jsonify({
            "timestamp": f"Архив за {date_str}",
            "system": {
                "c110": c110_val,
                "c50_formatted": c50_val,
                "c45_formatted": c45_val,
                "m45_allowed": False
            },
            "zones": zones_data,
            "current_bookmark": "АРХИВНЫЙ РЕЖИМ"
        })
    except Exception as e:
        print(f"!!! ОШИБКА АРХИВА STATE: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/archive/released')
def get_archive_released():
    """Получает список выпущенных (ist_ostan) за выбранную дату"""
    date_str = request.args.get('date')
    try:
        table_name = get_archive_table_name("ist_ostan", date_str)
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        query = f"SELECT na_lente, tek_stop FROM {table_name} WHERE na_lente IS NOT NULL AND DATALENGTH(na_lente) > 0 ORDER BY Id"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        released_list = []
        for row in rows:
            na_lente_str = str(row.na_lente).strip() if row.na_lente else ''
            if na_lente_str:
                released_list.append({
                    'na_lente': na_lente_str,
                    'tek_stop': str(row.tek_stop).strip() if row.tek_stop else ''
                })
        return jsonify(released_list)
    except Exception as e:
        print(f"!!! ОШИБКА АРХИВА RELEASED: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/archive/history/<zone_name>')
def get_archive_zone_history(zone_name):
    """Получает историю остановов (graf) за выбранную дату"""
    date_str = request.args.get('date')
    try:
        table_name = get_archive_table_name("graf", date_str)
        db_column = ZONE_TO_DB_COLUMN.get(zone_name, zone_name)
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        query = f"SELECT _data, {db_column} FROM {table_name} WHERE {db_column} IS NOT NULL ORDER BY Id"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        stops = []
        current_stop_start = None
        for row in rows:
            time_str = str(row._data).strip() if row._data else None
            value = int(row.__getitem__(1)) if row.__getitem__(1) is not None else 0
            
            if value == 1 and current_stop_start is None:
                current_stop_start = time_str
            elif value == 0 and current_stop_start is not None:
                stops.append({'start': current_stop_start, 'end': time_str, 'duration': calculate_duration(current_stop_start, time_str)})
                current_stop_start = None
                
        if current_stop_start is not None:
            stops.append({'start': current_stop_start, 'end': 'Конец дня', 'duration': 'Н/Д'})
            
        stops.reverse()
        return jsonify({'total_stops': len(stops), 'stops': stops})
    except Exception as e:
        print(f"!!! ОШИБКА АРХИВА HISTORY: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/archive/graf_data')
def get_archive_graf_data():
    """Получение всех данных из graf таблицы за выбранную дату"""
    date_str = request.args.get('date')
    try:
        table_name = get_archive_table_name("graf", date_str)
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        if cursor.fetchone()[0] == 0:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Таблица {table_name} не найдена.'}), 404
        
        db_columns = [ZONE_TO_DB_COLUMN[zone] for zone in ZONE_TO_DB_COLUMN.keys()]
        query = f"""
            SELECT Id, _data, {', '.join(db_columns)}
            FROM {table_name}
            ORDER BY Id
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        zone_names = list(ZONE_TO_DB_COLUMN.keys())
        data = []
        for row in rows:
            record = {'Id': row[0]}
            record['_data'] = str(row[1]).strip() if row[1] else None
            
            for i, zone in enumerate(zone_names):
                val = row[i + 2]
                record[zone] = int(val) if val is not None else 0
            
            data.append(record)
        
        return jsonify(data)
    
    except Exception as e:
        print(f"Ошибка в get_archive_graf_data: {e}")
        return jsonify({'error': str(e)}), 500

# ========================== OLD APP.PY ENDPOINTS ==========================

@app.route('/data')
def get_data():
    """Endpoint for plc.html - returns data for selected date"""
    date_str = request.args.get('date', datetime.now().strftime("%Y_%m_%d"))
    try:
        # Convert date format if needed
        if '-' in date_str:
            date_str = date_str.replace('-', '_')
        
        # Try to read from database tables
        table_name = f"data_on_line{date_str}"
        graf_table_name = f"graf_{date_str}"
        ist_table_name = f"ist_ostan_{date_str}"
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        if cursor.fetchone()[0] == 0:
            cursor.close()
            conn.close()
            return jsonify({"error": "Table not found"}), 404
        
        # Read data_on_line (3 rows)
        cursor.execute(f"""
            SELECT Id, _data, cep_inf, inf1_, inf2_, inf3_, inf4_, inf5_, inf6_, inf7_, inf8_, inf9_, inf10_, inf11_, inf12_ 
            FROM {table_name} WHERE Id IN (1, 2, 3)
        """)
        data_rows = cursor.fetchall()
        
        # Read graf data
        cursor.execute(f"""
            SELECT _data, cep_inf, inf1_, inf2_, inf3_, inf4_, inf5_, inf6_, inf7_, inf8_, inf9_, inf10_, inf11_, inf12_ 
            FROM {graf_table_name} ORDER BY Id
        """)
        graf_rows = cursor.fetchall()
        
        # Read ist_ostan data
        cursor.execute(f"SELECT na_lente, tek_stop FROM {ist_table_name} WHERE na_lente IS NOT NULL AND DATALENGTH(na_lente) > 0 ORDER BY Id")
        ist_rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Format data_on_line
        data_on_line = []
        for row in data_rows:
            data_on_line.append({
                "Id": row.Id,
                "_data": str(row._data).strip() if row._data else "",
                "cep_inf": str(row.cep_inf).strip() if row.cep_inf else "—",
                "inf1_": str(row.inf1_).strip() if row.inf1_ else "—",
                "inf2_": str(row.inf2_).strip() if row.inf2_ else "—",
                "inf3_": str(row.inf3_).strip() if row.inf3_ else "—",
                "inf4_": str(row.inf4_).strip() if row.inf4_ else "—",
                "inf5_": str(row.inf5_).strip() if row.inf5_ else "—",
                "inf6_": str(row.inf6_).strip() if row.inf6_ else "—",
                "inf7_": str(row.inf7_).strip() if row.inf7_ else "—",
                "inf8_": str(row.inf8_).strip() if row.inf8_ else "—",
                "inf9_": str(row.inf9_).strip() if row.inf9_ else "—",
                "inf10_": str(row.inf10_).strip() if row.inf10_ else "—",
                "inf11_": str(row.inf11_).strip() if row.inf11_ else "—",
                "inf12_": str(row.inf12_).strip() if row.inf12_ else "—",
            })
        
        # Format graf
        graf_ = []
        for row in graf_rows:
            graf_.append({
                "_data": str(row._data).strip() if row._data else "",
                "cep_inf": str(row.cep_inf).strip() if row.cep_inf else "0",
                "inf1_": str(row.inf1_).strip() if row.inf1_ else "0",
                "inf2_": str(row.inf2_).strip() if row.inf2_ else "0",
                "inf3_": str(row.inf3_).strip() if row.inf3_ else "0",
                "inf4_": str(row.inf4_).strip() if row.inf4_ else "0",
                "inf5_": str(row.inf5_).strip() if row.inf5_ else "0",
                "inf6_": str(row.inf6_).strip() if row.inf6_ else "0",
                "inf7_": str(row.inf7_).strip() if row.inf7_ else "0",
                "inf8_": str(row.inf8_).strip() if row.inf8_ else "0",
                "inf9_": str(row.inf9_).strip() if row.inf9_ else "0",
                "inf10_": str(row.inf10_).strip() if row.inf10_ else "0",
                "inf11_": str(row.inf11_).strip() if row.inf11_ else "0",
                "inf12_": str(row.inf12_).strip() if row.inf12_ else "0",
            })
        
        # Format ist_ostan
        list_ostan = []
        for row in ist_rows:
            list_ostan.append({
                "na_lente": str(row.na_lente).strip() if row.na_lente else "",
                "tek_stop": str(row.tek_stop).strip() if row.tek_stop else ""
            })
        
        return jsonify({
            "data_on_line": data_on_line,
            "graf_": graf_,
            "list_ostan": list_ostan
        })
        
    except Exception as e:
        print(f"Error in /data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/users')
def get_users_plc():
    """Endpoint for plc.html - returns active users from UserLogins table"""
    try:
        query = """
            SELECT 
                IPAddress AS ip,
                HostName AS hostname,
                LogTime AS last_seen_raw,
                AppName AS user_agent
            FROM yamid.dbo.UserLogins
            WHERE LogTime >= DATEADD(MINUTE, -30, GETDATE())
            ORDER BY LogTime DESC;
        """
        
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        active_users = []
        for row in rows:
            time_str = "—"
            if row.last_seen_raw:
                time_str = row.last_seen_raw.strftime("%H:%M:%S")
            
            active_users.append({
                "ip": str(row.ip),
                "hostname": str(row.hostname),
                "last_seen": time_str,
                "requests": "—",
                "user_agent": str(row.user_agent)[:60] if row.user_agent else "—"
            })
        
        return jsonify({"users": active_users})
        
    except Exception as e:
        print(f"Error in /users: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/scada')
def get_scada():
    """SCADA JSON endpoint with full tag data"""
    try:
        with PLC_LOCK:
            mc = Type3E()
            mc.connect(PLC_IP, PLC_PORTS[0])
            try:
                # Read all data
                rx = mc.batchread_bitunits("X0", 43)
                ry = mc.batchread_bitunits("Y0", 6)
                rl = mc.batchread_bitunits("L0", 58)
                rm = mc.batchread_bitunits("M0", 600)
                rc = mc.batchread_wordunits("CN30", 111)
                
                # Read D registers
                all_data_d = []
                for i in range(0, TOTAL_WORDS_D, CHUNK_SIZE):
                    size = min(CHUNK_SIZE, TOTAL_WORDS_D - i)
                    chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"D{i}", size)]
                    all_data_d.extend(chunk)
                
                # Read R registers
                all_data_rd = []
                for i in range(0, TOTAL_WORDS_RD, CHUNK_SIZE):
                    size = min(CHUNK_SIZE, TOTAL_WORDS_RD - i)
                    chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"R{i}", size)]
                    all_data_rd.extend(chunk)
                
                # Read SD
                SD_LIST = [0,200,201,203,600,604,606,607,608,609,610,611,612,210,211,212,213,214,215,216,519,523,524,525]
                max_sd = max(SD_LIST)
                sd_range = mc.batchread_wordunits("SD0", max_sd+1)
                rsd = {addr: sd_range[addr] & 0xFFFF for addr in SD_LIST}
                
                data = (
                    [bool(x) for x in rx],
                    [bool(y) for y in ry],
                    rc,
                    [bool(l) for l in rl],
                    all_data_d,
                    [bool(m) for m in rm],
                    rsd,
                    all_data_rd
                )
                
                scada_json = build_scada_json(data)
                return jsonify(scada_json)
            finally:
                mc.close()
    except Exception as e:
        print(f"Error in /scada: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/events')
def get_events():
    """Returns event log - supports from_file parameter for full history"""
    from_file = request.args.get('from_file', '0') == '1'
    
    try:
        if from_file:
            # Read from today's log file
            log_path = get_log_path()
            events = []
            parse_errors = []
            
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            parse_errors.append(f"Line {line_num}: {e}")
            
            # Sort by timestamp descending (newest first)
            events.sort(key=lambda x: x.get('ts', ''), reverse=True)
            
            return jsonify({
                "events": events,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "parse_errors": parse_errors
            })
        else:
            # Return in-memory events (all available, up to maxlen)
            with EVENT_LOCK:
                events = list(EVENT_LOG)
            return jsonify(events)
    except Exception as e:
        print(f"Error in /events: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/plc_raw')
def get_plc_raw():
    """Raw PLC data endpoint for plc.html"""
    try:
        mc = Type3E()
        mc.connect(PLC_IP, 5002)
        try:
            rx_bits = mc.batchread_bitunits("X0", 43)
            ry_bits = mc.batchread_bitunits("Y0", 6)
            rl_bits = mc.batchread_bitunits("L0", 58)
            c102_val = mc.batchread_wordunits("CN102", 1)[0]
            d714_val = mc.batchread_wordunits("D714", 1)[0]
            d4000_val = mc.batchread_wordunits("D4000", 1)[0]
            
            result = []
            
            # X
            for i, val in enumerate(rx_bits):
                addr_oct = format(i, "o")
                addr_dec = i
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
            
            result.append({"type": "C", "addr_dec": 102, "val": c102_val})
            result.append({"type": "D", "addr_dec": 714, "val": d714_val})
            result.append({"type": "D", "addr_dec": 4000, "val": d4000_val})
            
            return jsonify(result)
        finally:
            mc.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ========================== PLC_ADMIN ENDPOINTS ==========================

@app.route('/plc_get')
def get_plc_get():
    """PLC tags data endpoint for plc_admin.html - returns structured PLC data"""
    try:
        with PLC_LOCK:
            mc = Type3E()
            mc.connect(PLC_IP, PLC_PORTS[0])
            try:
                # Read all data
                rx = mc.batchread_bitunits("X0", 43)
                ry = mc.batchread_bitunits("Y0", 6)
                rl = mc.batchread_bitunits("L0", 58)
                rm = mc.batchread_bitunits("M0", 600)
                rc = mc.batchread_wordunits("CN30", 111)
                
                # Read D registers
                all_data_d = []
                for i in range(0, TOTAL_WORDS_D, CHUNK_SIZE):
                    size = min(CHUNK_SIZE, TOTAL_WORDS_D - i)
                    chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"D{i}", size)]
                    all_data_d.extend(chunk)
                
                # Read R registers
                all_data_rd = []
                for i in range(0, TOTAL_WORDS_RD, CHUNK_SIZE):
                    size = min(CHUNK_SIZE, TOTAL_WORDS_RD - i)
                    chunk = [v & 0xFFFF for v in mc.batchread_wordunits(f"R{i}", size)]
                    all_data_rd.extend(chunk)
                
                # Read SD
                SD_LIST = [0,200,201,203,600,604,606,607,608,609,610,611,612,210,211,212,213,214,215,216,519,523,524,525]
                max_sd = max(SD_LIST)
                sd_range = mc.batchread_wordunits("SD0", max_sd+1)
                rsd = {addr: sd_range[addr] & 0xFFFF for addr in SD_LIST}
                
                # Build tags in format expected by plc_admin.html
                tags = []
                
                # RX (X inputs)
                for i, val in enumerate(rx):
                    tags.append({
                        "type": "RX",
                        "addr": str(i),
                        "val": bool(val),
                        "name": TAGS.get("RX", {}).get(i, "")
                    })
                
                # RY (Y outputs)
                for i, val in enumerate(ry):
                    tags.append({
                        "type": "RY",
                        "addr": str(i),
                        "val": bool(val),
                        "name": TAGS.get("RY", {}).get(i, "")
                    })
                
                # RC (Counters)
                for i, val in enumerate(rc):
                    addr = 30 + i
                    tags.append({
                        "type": "RC",
                        "addr": str(addr),
                        "val": val,
                        "name": TAGS.get("RC", {}).get(addr, "")
                    })
                
                # RL (Logical alerts)
                for i, val in enumerate(rl):
                    tags.append({
                        "type": "RL",
                        "addr": str(i),
                        "val": bool(val),
                        "name": TAGS.get("RL", {}).get(i, "")
                    })
                
                # RM (Markers)
                for i, val in enumerate(rm):
                    tags.append({
                        "type": "RM",
                        "addr": str(i),
                        "val": bool(val),
                        "name": TAGS.get("RM", {}).get(i, "")
                    })
                
                # RD (D registers)
                for i, val in enumerate(all_data_d):
                    tags.append({
                        "type": "RD",
                        "addr": str(i),
                        "val": val,
                        "name": TAGS.get("RD", {}).get(i, "")
                    })
                
                # SD (Special registers)
                for addr, val in rsd.items():
                    tags.append({
                        "type": "SD",
                        "addr": str(addr),
                        "val": val,
                        "name": TAGS.get("SD", {}).get(addr, "")
                    })
                
                # FRD (R registers)
                for i, val in enumerate(all_data_rd):
                    tags.append({
                        "type": "FRD",
                        "addr": str(i),
                        "val": val,
                        "name": ""
                    })
                
                return jsonify({"PLC_01": {"tags": tags}})
            finally:
                mc.close()
    except Exception as e:
        print(f"Error in /plc_get: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/auth_check', methods=['POST'])
def auth_check():
    """Password authentication for plc_admin.html"""
    data = request.get_json(silent=True) or {}
    password = data.get("password", "")
    
    # Use the same password as plc_write
    ADVANCED_PASSWORD = "12345678"  # Default password, can be configured
    
    return jsonify({"ok": password == ADVANCED_PASSWORD})


@app.route('/plc_write', methods=['POST'])
def plc_write():
    """Write PLC registers - requires authentication"""
    data = request.get_json(silent=True) or {}
    password = data.get("password", "")
    
    ADVANCED_PASSWORD = "12345678"
    if password != ADVANCED_PASSWORD:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    reg_type = str(data.get("type", "")).upper().strip()
    addr = data.get("addr")
    value = data.get("value")

    try:
        addr = int(addr)
        value = int(value)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "bad_input"}), 400

    word_types = {"D", "R", "C"}
    bit_types = {"M", "X", "Y", "L"}

    if reg_type in word_types:
        if not (0 <= value <= 0xFFFF):
            return jsonify({"ok": False, "error": "value_range"}), 400
        device = f"{reg_type}{addr}"
        if reg_type == "C":
            device = f"CN{addr}"
        mc = Type3E()
        try:
            mc.connect(PLC_IP, 5000)
            mc.batchwrite_wordunits(device, [value])
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
        finally:
            mc.close()

    if reg_type in bit_types:
        if value not in (0, 1):
            return jsonify({"ok": False, "error": "value_range"}), 400
        device = f"{reg_type}{addr}"
        mc = Type3E()
        try:
            mc.connect(PLC_IP, 5000)
            mc.batchwrite_bitunits(device, [value])
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
        finally:
            mc.close()

    return jsonify({"ok": False, "error": "unknown_type"}), 400


@app.route('/event_log')
def get_event_log():
    """Event log endpoint for plc_admin.html"""
    from_file = request.args.get('from_file', '0') == '1'
    
    try:
        if from_file:
            # Read from today's log file
            log_path = get_log_path()
            events = []
            parse_errors = []
            
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            parse_errors.append(f"Line {line_num}: {e}")
            
            # Sort by timestamp descending (newest first)
            events.sort(key=lambda x: x.get('ts', ''), reverse=True)
            
            return jsonify({
                "events": events,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "parse_errors": parse_errors
            })
        else:
            # Return in-memory events (last 100)
            with EVENT_LOCK:
                events = list(EVENT_LOG)[:100]
            return jsonify({"events": events})
    except Exception as e:
        print(f"Error in /event_log: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/cleanup_logs', methods=['POST'])
def cleanup_logs():
    """Cleanup old log files"""
    try:
        deleted = cleanup_old_logs(keep_days=1)
        return jsonify({"deleted_files": deleted})
    except Exception as e:
        print(f"Error in /cleanup_logs: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/event_log_dates')
def get_event_log_dates():
    """Returns list of available event log dates"""
    try:
        base, ext = os.path.splitext(EVENT_LOG_PATH)
        log_dir = os.path.dirname(EVENT_LOG_PATH) or "."
        
        if not os.path.exists(log_dir):
            return jsonify({"dates": []})
        
        dates = []
        pattern = os.path.join(log_dir, f"{os.path.basename(base)}_*.json")
        
        for filepath in glob.glob(pattern):
            try:
                filename = os.path.basename(filepath)
                date_part = filename.replace(f"{os.path.basename(base)}_", "").replace(".json", "")
                file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                
                # Get file size
                file_size = os.path.getsize(filepath)
                
                dates.append({
                    "date": date_part,
                    "date_formatted": file_date.strftime("%d.%m.%Y"),
                    "file_size": file_size
                })
            except (ValueError, Exception):
                continue
        
        # Sort by date descending (newest first)
        dates.sort(key=lambda x: x["date"], reverse=True)
        
        return jsonify({"dates": dates})
    except Exception as e:
        print(f"Error in /api/event_log_dates: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/event_log_by_date')
def get_event_log_by_date():
    """Возвращает лог событий для конкретной даты в хронологическом порядке."""
    date_str = request.args.get('date')
    if not date_str or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        
    try:
        # Получаем путь к логу (используйте вашу функцию, например get_timeline_log_path или get_log_path)
        log_path = get_log_path(datetime.strptime(date_str, "%Y-%m-%d"))
        events = []
        parse_errors = []
        
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        parse_errors.append(f"Line {line_num}: {e}")
                        
        # 🔥 УБРАНА СОРТИРОВКА: отдаем данные от старых к новым.
        # Фронтенд сам развернет их при отрисовке, зато сравнение бит теперь сработает идеально.
        
        return jsonify({
            "events": events,
            "date": date_str,
            "parse_errors": parse_errors
        })
    except Exception as e:
        logger.error(f"Error in /api/event_log_by_date: {e}")
        return jsonify({"error": str(e)}), 500

import glob # Убедитесь, что этот импорт есть вверху файла

@app.route('/api/available_dates', methods=['GET'])
def get_available_dates():
    """Возвращает список дат, за которые реально существуют лог-файлы (с поддержкой .gz)"""
    try:
        # Находим обычные файлы .jsonl
        pattern_jsonl = os.path.join(TIMELINE_LOG_DIR, f"{LOG_FILE_PREFIX}*.jsonl")
        files_jsonl = glob.glob(pattern_jsonl)
        
        # Находим сжатые файлы .jsonl.gz
        pattern_gz = os.path.join(TIMELINE_LOG_DIR, f"{LOG_FILE_PREFIX}*.jsonl.gz")
        files_gz = glob.glob(pattern_gz)
        
        # Объединяем оба списка файлов
        all_files = files_jsonl + files_gz
        
        dates = []
        for filepath in all_files:
            filename = os.path.basename(filepath)
            # Отрезаем префикс и любые варианты расширений (.jsonl или .jsonl.gz)
            date_str = filename.replace(LOG_FILE_PREFIX, "").replace(".jsonl.gz", "").replace(".jsonl", "")
            
            if date_str not in dates:
                dates.append(date_str)
                
        # Сортируем даты по убыванию, как было у вас
        dates.sort(reverse=True)
        
        # Возвращаем строго в том формате, который ждет фронтенд!
        return jsonify({"dates": dates})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/plc_XYL')
def plc_XYL():
    """PLC X/Y/L/C/D data endpoint for plc_admin.html conveyor monitoring"""
    try:
        mc = Type3E()
        mc.connect(PLC_IP, 5003)

        rx_bits = mc.batchread_bitunits("X0", 43)
        if not rx_bits:
            return jsonify({"error": "Failed to read X bits"}), 500
        ry_bits = mc.batchread_bitunits("Y0", 6)
        rl_bits = mc.batchread_bitunits("L0", 58)
        c102_val = mc.batchread_wordunits("CN102", 1)[0]
        d714_val = mc.batchread_wordunits("D714", 1)[0]
        d4000_val = mc.batchread_wordunits("D4000", 1)[0]

        result = []

        # X
        for i, val in enumerate(rx_bits):
            addr_oct = format(i, "o")
            addr_dec = i
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
        # C and D
        result.append({"type": "C", "addr_dec": 102, "val": c102_val})
        result.append({"type": "D", "addr_dec": 714, "val": d714_val})
        result.append({"type": "D", "addr_dec": 4000, "val": d4000_val})

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        mc.close()


# ========================== MAIN ==========================
if __name__ == '__main__':
    print(f"✅ Строка подключения сформирована. Запуск сервера...")
    
    # Загружаем сохранённое состояние событий ПЕРЕД запуском потока опроса
    load_event_state()
    
    # Cleanup old logs on startup - use LOG_RETENTION_DAYS (10) to keep 10 days of history
    cleanup_old_logs(keep_days=LOG_RETENTION_DAYS)
    
    # Start event polling thread
    # threading.Thread(target=event_poll_loop, daemon=True).start()
    threading.Thread(target=event_poll_loop_from_json, daemon=True).start()
    # Сохраняем состояние при выходе
    import atexit
    atexit.register(save_event_state)
    
    app.run(host='0.0.0.0', port=80, debug=False)
