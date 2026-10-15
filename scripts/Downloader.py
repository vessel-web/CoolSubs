#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════════════════
#
#   Downloader — сборщик конфигов из ссылок-подписок.
#
#   Программа читает sources.txt, скачивает каждый источник и вытаскивает
#   из него конфиги: прямые ссылки, Base64 любой вложенности, JSON
#   Xray/sing-box и YAML Clash. Повторы отбрасываются, новое дописывается
#   в temp/configs.txt.
#
#   Как работает чёрный список. Источник, который долго не отвечает, не
#   всегда мёртв — возможно, он просто медленный. Если источник уже успел
#   отдать хоть сколько-то конфигов, в ЧС его отправлять нельзя. В ЧС
#   едут только те, от кого не удалось получить ни одного конфига, и то
#   с учётом выбранного метода:
#     • «Каждый раз» — ошибка значит сразу в ЧС;
#     • «Кулдаун» — в ЧС не чаще одного раза в N часов (по умолчанию 24),
#       между отправками источник продолжает пробовать.
#
#   Порядок частей в файле:
#     ПУТИ           — расположение sources.txt, configs.txt, настроек, логов
#     КОНСОЛЬ        — цветной потокобезопасный вывод и прогресс-бар
#     НАСТРОЙКИ      — dataclass, загрузка/сохранение JSON, пункты меню
#     ДЛИТЕЛЬНОСТИ   — показ «2 часа» вместо 7200 сек и разбор такого ввода
#     ФАЙЛЫ          — чтение, атомарная запись, память процесса
#     ОТЛАДКА        — подробный журнал в logs/Downloader (включается настройкой)
#     КУЛДАУН ЧС     — когда источник последний раз уезжал в чёрный список
#     ПАРСЕР         — валидация, нормализация, Base64/JSON/YAML (потоково)
#     СЕТЬ           — requests-сессия на поток, стриминговое чтение ответа
#     GITHUB-ФИКС    — замена хэша коммита в raw-ссылке на ветку main/master
#     ХРАНИЛИЩЕ      — дедупликация configs.txt по BLAKE2b
#     ПУЛ ПОТОКОВ    — daemon-воркеры: зависшие не держат цикл и выход
#     ЦИКЛ ЗАГРУЗКИ  — скользящее окно, дедлайны, фоновый добор, чёрный список
#     КОНВЕЙЕР       — круглосуточный режим: добор перед отдыхом и подхват
#                      новых источников прямо во время отдыха
#     ИНТЕРФЕЙС      — логотип, главное меню, меню настроек
#     ЗАПУСК         — App, argparse, main()
#
# ══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import argparse
import base64
import gc
import hashlib
import ipaddress
import json
import os
import queue
import random
import re
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import requests
import urllib3
import yaml

urllib3.disable_warnings()  # запросы идут с verify=False — ругань на каждый из них не нужна

# ══════════════════════════════════════════════════════════════════════════════
# ПУТИ (всё относительно корня — папки, где лежит этот файл)
# ══════════════════════════════════════════════════════════════════════════════

ROOT_DIR: Path = Path(__file__).resolve().parent.parent
SOURCES_DIR: Path = ROOT_DIR / "sources"
TEMP_DIR: Path = ROOT_DIR / "temp"
DOWNLOADER_DIR: Path = TEMP_DIR / "Downloader"
LOGS_DIR: Path = ROOT_DIR / "logs"
DEBUG_DIR: Path = LOGS_DIR / "Downloader"

SOURCES_FILE: Path = SOURCES_DIR / "sources.txt"
# Чёрный список лежит в sources/, ровно там, где его читает
# Telegram_Checker.py. Файл должен быть один, иначе связка ЧС между
# скриптами не работает.
BLACKLIST_FILE: Path = SOURCES_DIR / "blacklist_sources.txt"
CONFIGS_FILE: Path = TEMP_DIR / "configs.txt"
SETTINGS_FILE: Path = DOWNLOADER_DIR / "settings.json"
# Таблица кулдауна ЧС — построчный текст «url<TAB>время» рядом с
# настройками. Старый blacklist_cooldown.json при первом запуске
# конвертируется в этот файл и удаляется.
COOLDOWN_FILE: Path = DOWNLOADER_DIR / "blacklist_cooldown.txt"
# Advisory-замок sources.txt: этот скрипт перезаписывает файл, а
# Telegram_Checker/telegram_sub_bot дописывают в него новые ссылки.
SOURCES_LOCK_FILE: Path = TEMP_DIR / "sources.lock"

# Откуда качать список источников, если свой sources.txt потерялся.
# Меняется в настройках (sources_fallback_url), это значение — запасное.
DEFAULT_SOURCES_URL: str = (
    "https://raw.githubusercontent.com/RKPchannel/RKP_bypass_configs"
    "/refs/heads/main/sources.txt"
)

# Невидимые символы, которыми грешат скопированные откуда попало списки:
# строка «\u200bhttps://...» обычным strip() не чистится и выпадает из работы.
_ZERO_WIDTH: str = "\u200b\u200c\u200d\u2060\ufeff"


def clean_source_line(line: str) -> str:
    """Строка источника без мусора по краям: пробелы и невидимые символы."""
    return line.strip().strip(_ZERO_WIDTH).strip()


def ensure_dirs() -> None:
    """Создаёт папки и пустые обязательные файлы при первом запуске."""
    for directory in (SOURCES_DIR, TEMP_DIR, DOWNLOADER_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    for file in (BLACKLIST_FILE, CONFIGS_FILE):
        if not file.exists():
            file.touch()
    # sources.txt намеренно не создаём пустым: если его нет, цикл сам
    # скачает список из интернета (см. Downloader._ensure_sources_file).


# ══════════════════════════════════════════════════════════════════════════════
# КОНСОЛЬ: цвета, уровни логирования, прогресс-строка
# ══════════════════════════════════════════════════════════════════════════════

LEVELS: dict[str, int] = {"quiet": 0, "normal": 1, "verbose": 2}

COLOR_CODES: dict[str, str] = {
    "cyan": "\033[96m", "green": "\033[92m", "yellow": "\033[93m",
    "red": "\033[91m", "blue": "\033[94m", "magenta": "\033[95m",
    "white": "\033[97m", "gray": "\033[90m",
}
_RESET = "\033[0m"
_BOLD = "\033[1m"
_CLEAR_LINE = "\r\033[2K"


class Console:
    """Потокобезопасный цветной вывод в stdout.

    Поддерживает уровни quiet/normal/verbose, цветовую схему с настраиваемым
    акцентом, полную очистку экрана и «живую» строку прогресса: обычные
    сообщения печатаются поверх прогресса, а сам прогресс сразу
    перерисовывается — экран не мерцает.

    Служебные сообщения (перемещение источников в чёрный список и прочая
    бухгалтерия) в обычном режиме не показываются: они сыпались на каждый
    чих и забивали экран. Для них есть метод debug() — серый текст,
    видимый только в подробном режиме (verbose) и в отладочном журнале.
    """

    def __init__(self, level: str = "normal", use_colors: bool = True, accent: str = "cyan") -> None:
        if os.name == "nt":
            os.system("")  # включает обработку ANSI-кодов в терминалах Windows
        self._lock = threading.Lock()
        self._level: int = LEVELS.get(level, 1)
        self._accent: str = accent if accent in COLOR_CODES else "cyan"
        self.use_colors: bool = use_colors and sys.stdout.isatty()
        self._progress_text: str | None = None  # последняя строка прогресса

    def set_level(self, level: str) -> None:
        self._level = LEVELS.get(level, 1)

    def set_colors(self, flag: bool) -> None:
        self.use_colors = flag and sys.stdout.isatty()

    def set_accent(self, name: str) -> None:
        new = name if name in COLOR_CODES else "cyan"
        if new != self._accent:
            global _LOGO_ANIMATE
            _LOGO_ANIMATE = True    # после смены цвета логотип показывается заново
        self._accent = new

    @property
    def accent(self) -> str:
        """Имя текущего акцентного цвета (для покраски пунктов меню)."""
        return self._accent

    # --------------------------------------------------------------- покраска
    def _paint(self, text: str, color: str | None = None, bold: bool = False) -> str:
        """Оборачивает текст ANSI-кодами цвета (если цвета включены)."""
        if not self.use_colors:
            return text
        prefix = (_BOLD if bold else "") + (COLOR_CODES.get(color, "") if color else "")
        return f"{prefix}{text}{_RESET}" if prefix else text

    def paint(self, text: str, color: str | None = None, bold: bool = False) -> str:
        """Публичная покраска текста (если цвета включены)."""
        return self._paint(text, color, bold)

    def bar(self, done: int, total: int, width: int = 20) -> str:
        """Прогресс-бар вида |████░░░░| (заполнение — цвет интерфейса, фон — серый)."""
        frac = 1.0 if total <= 0 else min(1.0, max(0.0, done / total))
        filled = int(round(frac * width))
        core = self._paint("█" * filled, self._accent) + self._paint("░" * (width - filled), "gray")
        return self._paint("|", "gray") + core + self._paint("|", "gray")

    def clear_screen(self) -> None:
        """Полная очистка экрана и возврат курсора в левый верхний угол.

        На Windows используется команда cls: она работает в любой консоли,
        включая оболочки вроде PyCharm, где ANSI-очистка не срабатывает.
        На остальных системах — тот же ANSI-код через stdout.
        """
        with self._lock:
            self._progress_text = None
        if os.name == "nt":
            os.system("cls")
        else:
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.flush()

    # --------------------------------------------------------------- строки
    def _write_progress_locked(self) -> None:
        if self._progress_text is None:
            return
        sys.stdout.write(_CLEAR_LINE + self._progress_text + "\033[K")
        sys.stdout.flush()

    def _emit(self, text: str, color: str | None, min_level: int, bold: bool = False) -> None:
        """Печатает строку, временно убирая и восстанавливая строку прогресса."""
        with self._lock:
            if self._level < min_level:
                return
            had_progress = self._progress_text is not None
            if had_progress:
                sys.stdout.write(_CLEAR_LINE)
            sys.stdout.write(self._paint(text, color, bold) + "\n")
            if had_progress:
                self._write_progress_locked()  # прогресс не гаснет — сразу перерисовываем
            else:
                sys.stdout.flush()

    def always(self, text: str = "") -> None:            # видно даже в quiet
        self._emit(text, None, 0)

    def error(self, text: str) -> None:
        self._emit("[ОШИБКА] " + text, "red", 0)

    def warn(self, text: str) -> None:
        self._emit("[!] " + text, "yellow", 1)

    def info(self, text: str) -> None:
        self._emit(text, None, 1)

    def ok(self, text: str) -> None:
        self._emit("[OK] " + text, "green", 1)

    def verbose(self, text: str, *, ok: bool = False) -> None:
        self._emit(text, "green" if ok else "gray", 2)

    def debug(self, text: str) -> None:
        """Служебная строка — только в подробном режиме, серым."""
        self._emit(text, "gray", 2)

    def header(self, text: str) -> None:
        self._emit(text, self._accent, 0, bold=True)

    def banner(self, text: str) -> None:
        """Многострочный логотип — виден всегда; первый показ — с анимацией."""
        if _logo_first_show(self, text):
            return
        self._emit(text, self._accent, 0, bold=True)

    # --------------------------------------------------------------- прогресс
    def progress(self, text: str) -> None:
        """Перерисовывает однострочный прогресс (текст уже покрашен вызывающим)."""
        with self._lock:
            if self._level < 1:
                return
            self._progress_text = text
            self._write_progress_locked()

    def clear_progress(self) -> None:
        with self._lock:
            if self._progress_text is not None:
                sys.stdout.write(_CLEAR_LINE)
                sys.stdout.flush()
                self._progress_text = None


# ══════════════════════════════════════════════════════════════════════════════
# НАСТРОЙКИ: dataclass + JSON + спецификации пунктов для меню
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
# Версия Happ для User-Agent и X-App-Version. Клиент обновляется часто:
# при смене версии достаточно поправить эту константу или перегенерировать
# устройство пунктом меню «Сгенерировать X-HWID».
DEFAULT_HAPP_VERSION: str = "4.6.0"

# Пул генератора устройства (меню «Сгенерировать X-HWID»): модели — как
# Build.MODEL настоящего Android, версии ОС и Happ повторяются чаще —
# это сдвигает выбор в сторону свежих версий.
ANDROID_MODELS: tuple[str, ...] = (
    "SM-S918B", "SM-S921B", "SM-S928B",         # Galaxy S23 Ultra / S24 / S24 Ultra
    "SM-A146P", "SM-A546B", "SM-A556B",         # Galaxy A14 / A54 / A55
    "Pixel 7", "Pixel 8", "Pixel 8 Pro",
    "M2101K6G", "2201123G", "23127PN0CC",       # Redmi Note 10 Pro / Xiaomi 12 / Xiaomi 14
    "CPH2451",                                   # OnePlus 11
)
ANDROID_OS_VERSIONS: tuple[str, ...] = ("12", "13", "14", "14", "15", "15")
HAPP_APP_VERSIONS: tuple[str, ...] = ("4.5.0", "4.6.0", "4.6.0", "4.6.0")


def generate_happ_device() -> dict[str, str]:
    """Случайное устройство «под Happ»: HWID, модель, версия ОС, User-Agent.

    64-битное число живёт в двух видах, как у настоящего клиента: 16
    шестнадцатеричных символов — X-HWID и параметр ?hwid=, и другое такое
    же число в десятичной записи — хвост User-Agent (Happ/<версия>/Android/
    <число>). Старший бит выставлен, чтобы hex не начинался с нуля, а
    десятичная запись была 19–20 цифр, как настоящий ANDROID_ID. Результат
    никуда не пишется сам: раскладку по настройкам и сохранение делает меню.
    """
    app = random.choice(HAPP_APP_VERSIONS)
    return {
        "hwid": f"{random.getrandbits(63) | (1 << 63):016x}",
        "model": random.choice(ANDROID_MODELS),
        "os": random.choice(ANDROID_OS_VERSIONS),
        "app": app,
        "ua_number": str(random.getrandbits(63) | (1 << 63)),
    }


def happ_user_agent(settings: "Settings") -> str:
    """Собирает User-Agent Happ из частей настроек (пустой номер — без хвоста)."""
    version = settings.happ_app_version or DEFAULT_HAPP_VERSION
    if settings.happ_ua_number:
        return f"Happ/{version}/Android/{settings.happ_ua_number}"
    return f"Happ/{version}/Android"

_TRUE_WORDS = {"1", "true", "yes", "on", "д", "да", "вкл", "y"}
_FALSE_WORDS = {"0", "false", "no", "off", "н", "нет", "выкл", "n"}


@dataclass(frozen=True)
class ParamSpec:
    """Описание одного параметра настроек — для меню и приведения типов.

    kind — как параметр ведёт себя в меню:
      bool     — переключатель: выбор пункта сразу меняет Да на Нет и
                 обратно, ничего вводить не нужно;
      int      — целое число;
      duration — время: хранится в секундах, показывается по-человечески
                 («2 часа»), вводится с суффиксом (2ч, 1ч30м, 45м, 90с);
      str      — произвольная строка;
      choice   — выбор из вариантов: в меню показываются русские подписи
                 (labels), а хранится и вводится английское значение
                 (verbose) или номер пункта — с английской раскладки
                 русское слово не набрать, а видеть в консоли хочется
                 именно русское;
      action   — не параметр, а команда меню (генерация HWID и т.п.);
      info     — только показ: значение считается на месте, редактировать
                 его нельзя (например, собранный User-Agent Happ).
    unit — надпись единицы для отображения («hours» — число показывается
                 как «24 часа», а не голой цифрой).
    """
    key: str
    title: str
    kind: str
    minimum: int | None = None
    maximum: int | None = None
    choices: tuple[str, ...] | None = None
    labels: tuple[str, ...] | None = None
    zero_means: str = ""  # подпись вместо «0 сек» (например «без автоповтора»)
    unit: str = ""        # "hours" — показывать целое число как часы


@dataclass(frozen=True)
class CategorySpec:
    title: str
    params: tuple[ParamSpec, ...]


CATEGORIES: tuple[CategorySpec, ...] = (
    CategorySpec("Сохранение и нормализация конфигов", (
        ParamSpec("save_vless", "Сохранять VLESS", "bool"),
        ParamSpec("save_vmess", "Сохранять VMess", "bool"),
        ParamSpec("save_ss", "Сохранять Shadowsocks", "bool"),
        ParamSpec("save_trojan", "Сохранять конфиги Trojan", "bool"),
        ParamSpec("save_hy2", "Сохранять Hysteria2", "bool"),
        ParamSpec("save_tuic", "Сохранять конфиги TUIC", "bool"),
        ParamSpec("normalize_ss_hy2_aliases",
                  "Нормализация shadowsocks://→ss:// и hy2://→hysteria2://", "bool"),
        ParamSpec("max_url_length", "Макс. длина URL конфига", "int", 256, 65_536),
        ParamSpec("max_query_params", "Макс. число query-параметров", "int", 0, 128),
    )),
    CategorySpec("Декодирование содержимого", (
        ParamSpec("decode_base64", "Декодировать Base64 (в т.ч. многоуровневый)", "bool"),
        ParamSpec("base64_decode_levels", "Уровней вложенности Base64", "int", 1, 5),
        ParamSpec("decode_json", "Парсить JSON (Xray/V2Ray, sing-box, Clash)", "bool"),
        ParamSpec("decode_yaml", "Парсить YAML (Clash)", "bool"),
        ParamSpec("decrypt_happ", "Декодирование содержимого Happ методом", "bool"),
        ParamSpec("base64_max_len_mb", "Макс. размер Base64-блока, МБ", "int", 1, 128),
        ParamSpec("json_yaml_max_len_mb", "Макс. размер JSON/YAML, МБ", "int", 1, 128),
        ParamSpec("max_response_mb", "Макс. размер ответа сервера, МБ", "int", 1, 512),
    )),
    CategorySpec("Скорость и лимиты", (
        ParamSpec("http_threads", "Потоков загрузки", "int", 1, 200),
        ParamSpec("github_fix_threads", "Потоков GitHub-замены хэша на main/master", "int", 1, 100),
        ParamSpec("source_attempts", "Попыток на источник (при сбоях сети)", "int", 1, 5),
        ParamSpec("timeout_normal", "Таймаут соединения", "duration", 1, 60),
        ParamSpec("timeout_read", "Таймаут чтения ответа", "duration", 5, 600),
        ParamSpec("timeout_happ", "Таймаут Happ-метода (соединение и ответ)", "duration", 1, 60),
    )),
    CategorySpec("Циклы и дедлайны", (
        ParamSpec("rest_seconds", "Отдых между циклами (0 — без автоповтора)",
                  "duration", 0, 86_400, zero_means="без автоповтора"),
        ParamSpec("stall_warn_seconds", "Предупреждение «нет прогресса» через",
                  "duration", 30, 3600),
        ParamSpec("source_deadline_seconds", "Дедлайн на один источник (0 — отключить)",
                  "duration", 0, 3600, zero_means="отключено"),
        ParamSpec("max_download_seconds",
                  "Макс. время скачивания одного источника (0 — без лимита)",
                  "duration", 0, 3600, zero_means="без лимита"),
        ParamSpec("max_queue_wait_seconds",
                  "Макс. ожидание источника в очереди (0 — сколько потребуется)",
                  "duration", 0, 3600, zero_means="сколько потребуется"),
    )),
    CategorySpec("Конвейерная загрузка", (
        ParamSpec("conveyor_poll_seconds",
                  "Как часто смотреть sources.txt во время отдыха",
                  "duration", 3, 300),
        ParamSpec("conveyor_batch_wait_seconds",
                  "Накопление пачки новых ссылок перед мини-циклом (0 — сразу)",
                  "duration", 0, 600, zero_means="сразу, без накопления"),
        ParamSpec("conveyor_drain_before_rest",
                  "Добор новых источников перед уходом в отдых", "bool"),
        ParamSpec("conveyor_process_during_rest",
                  "Подхват и обработка новых источников прямо в отдыхе", "bool"),
        ParamSpec("conveyor_keep_rest_schedule",
                  "После мини-цикла продолжать оставшийся отдых (не сбрасывать таймер)", "bool"),
        ParamSpec("conveyor_max_mini_urls",
                  "Лимит новых ссылок за один мини-цикл (0 — без лимита)",
                  "int", 0, 100_000, zero_means="без лимита"),
        ParamSpec("conveyor_mini_threads",
                  "Потоков на мини-цикл (0 — как в основном цикле)",
                  "int", 0, 200, zero_means="как в основном цикле"),
        ParamSpec("conveyor_drain_max_rounds",
                  "Раундов добора подряд перед отдыхом", "int", 1, 50),
    )),
    CategorySpec("Источники и чёрный список", (
        ParamSpec("auto_download_sources",
                  "Скачать sources.txt из интернета, если нет своих источников", "bool"),
        ParamSpec("sources_fallback_url", "Откуда качать список источников", "str"),
        ParamSpec("fix_github_urls", "Исправлять GitHub raw-ссылки (хэш → main/master)", "bool"),
        ParamSpec("save_failed_sources", "Отправлять неудачные источники в чёрный список", "bool"),
        ParamSpec("blacklist_mode", "Чёрный список: метод добавления", "choice",
                  choices=("always", "cooldown"),
                  labels=("Каждый раз при ошибке", "Кулдаун — не чаще раза в N часов")),
        ParamSpec("blacklist_cooldown_hours",
                  "Чёрный список: кулдаун, часов (для метода «Кулдаун»)",
                  "int", 1, 720, unit="hours"),
        ParamSpec("verify_ssl", "Проверять SSL-сертификаты", "bool"),
    )),
    CategorySpec("User-Agent'ы и X-HWID", (
        ParamSpec("user_agent", "User-Agent для обычных загрузок", "str"),
        ParamSpec("happ_hwid", "X-HWID устройства Happ", "str"),
        ParamSpec("happ_device_model", "Модель устройства Happ (X-Device-Model)", "str"),
        ParamSpec("happ_device_os", "Версия ОС устройства Happ (X-Ver-OS)", "str"),
        ParamSpec("happ_app_version", "Версия Happ (X-App-Version и User-Agent)", "str"),
        ParamSpec("happ_ua_number", "Число в хвосте User-Agent Happ", "str"),
        ParamSpec("action_generate_hwid", "Сгенерировать X-HWID и устройство", "action"),
        ParamSpec("happ_ua_display", "User-Agent для Happ метода", "info"),
        ParamSpec("action_clear_hwid", "Удалить X-HWID и устройство", "action"),
    )),
    CategorySpec("Вывод и отладка", (
        ParamSpec("log_level", "Уровень логирования", "choice",
                  choices=("verbose", "normal", "quiet"),
                  labels=("Подробный", "Обычный", "Тихий")),
        ParamSpec("use_colors", "ANSI-цвета в консоли", "bool"),
        ParamSpec("accent_color", "Основной цвет интерфейса", "choice",
                  choices=("cyan", "green", "yellow", "red", "blue", "magenta", "white"),
                  labels=("Голубой", "Зелёный", "Жёлтый", "Красный",
                          "Синий", "Пурпурный", "Белый")),
        ParamSpec("show_progress_bar", "Прогресс-бар во время цикла", "bool"),
        ParamSpec("clear_screen", "Очищать консоль при переходах", "bool"),
        ParamSpec("show_memory_usage", "Показывать использование памяти", "bool"),
    )),
    CategorySpec("Логи и диагностика (для разработчиков)", (
        ParamSpec("debug_logs", "Подробный журнал в logs/Downloader", "bool"),
        ParamSpec("enable_faulthandler", "Faulthandler (лог падений)", "bool"),
    )),
)

ALL_PARAMS: dict[str, ParamSpec] = {
    spec.key: spec for category in CATEGORIES for spec in category.params
}


@dataclass
class Settings:
    # Значения по умолчанию — ровно то, что видно в меню при первом запуске
    # и после сброса. При первом старте весь набор уезжает в settings.json,
    # дальше файл правится через меню или руками.
    # Сохранение и нормализация конфигов
    save_vless: bool = True
    save_trojan: bool = True
    save_ss: bool = True
    save_vmess: bool = True
    save_hy2: bool = True
    save_tuic: bool = True
    # Декодирование содержимого
    decode_base64: bool = True
    decode_json: bool = True
    decode_yaml: bool = True
    decrypt_happ: bool = True
    base64_decode_levels: int = 3
    # Скорость и лимиты. Потоков по умолчанию достаточно для обычной
    # домашней машины; на слабом железе (2 ядра, немного ОЗУ) их стоит
    # убавить в настройках — нагрузка на ЦП и память заметно снизится.
    # Лимиты ответов сдержанные: пики памяти не выходят за несколько
    # сотен мегабайт даже на быстрых подписках.
    http_threads: int = 30
    github_fix_threads: int = 15
    source_attempts: int = 1          # повторы имеет смысл включать на плохой сети
    timeout_normal: int = 6
    timeout_read: int = 30
    timeout_happ: int = 6
    max_configs_per_source: int = 100_000
    base64_max_len_mb: int = 32
    json_yaml_max_len_mb: int = 32
    max_response_mb: int = 128
    # Циклы и дедлайны
    rest_seconds: int = 7200          # пауза автоповтора — 2 часа
    stall_warn_seconds: int = 30
    source_deadline_seconds: int = 30
    max_download_seconds: int = 120   # предел времени на скачивание одного ответа (0 — без лимита)
    max_queue_wait_seconds: int = 0   # предел ожидания в очереди (0 — сколько потребуется)
    # Источники и чёрный список
    fix_github_urls: bool = True
    save_failed_sources: bool = True
    blacklist_mode: str = "cooldown"  # always — каждый раз, cooldown — раз в N часов
    blacklist_cooldown_hours: int = 24
    auto_download_sources: bool = True
    sources_fallback_url: str = DEFAULT_SOURCES_URL
    user_agent: str = DEFAULT_USER_AGENT
    # Устройство для Happ-захода. По умолчанию ПУСТО: никакого вшитого
    # HWID в коде нет. Пока пользователь не сгенерирует устройство пунктом
    # меню «Сгенерировать X-HWID», заход идёт только с User-Agent Happ.
    # После генерации устройство хранится здесь и используется, пока его
    # не удалят или не сгенерируют заново.
    happ_hwid: str = ""
    happ_device_model: str = ""
    happ_device_os: str = ""
    happ_app_version: str = ""
    happ_ua_number: str = ""
    verify_ssl: bool = False
    # Вывод и отладка
    log_level: str = "normal"         # verbose показывает строку по каждому источнику
    use_colors: bool = True
    accent_color: str = "blue"
    show_progress_bar: bool = True
    clear_screen: bool = True
    show_memory_usage: bool = True
    # Логи и диагностика (для разработчиков)
    enable_faulthandler: bool = False
    debug_logs: bool = False          # подробный журнал в logs/Downloader
    # Нормализация и лимиты парсера
    normalize_ss_hy2_aliases: bool = True
    max_url_length: int = 8_192
    max_query_params: int = 64
    yaml_node_limit: int = 100_000
    # Конвейерная загрузка
    conveyor_poll_seconds: int = 10          # как часто смотреть sources.txt в отдыхе
    conveyor_batch_wait_seconds: int = 30    # накопление пачки новых ссылок
    conveyor_drain_before_rest: bool = True  # добор новых источников перед отдыхом
    conveyor_process_during_rest: bool = True  # подхват новых прямо в отдыхе
    conveyor_keep_rest_schedule: bool = True # отдых не сбрасывается мини-циклом
    conveyor_max_mini_urls: int = 0          # лимит ссылок на мини-цикл (0 — без лимита)
    conveyor_mini_threads: int = 0           # потоков на мини-цикл (0 — как в цикле)
    conveyor_drain_max_rounds: int = 1       # раундов добора перед отдыхом

    def to_dict(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}


def coerce_value(spec: ParamSpec | None, value: Any) -> Any:
    """Приводит значение к типу параметра; при ошибке бросает ValueError/TypeError."""
    kind = spec.kind if spec is not None else "str"
    if kind == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in _TRUE_WORDS:
                return True
            if text in _FALSE_WORDS:
                return False
        raise ValueError(f"некорректное булево значение: {value!r}")
    if kind in ("int", "duration"):
        if isinstance(value, bool):
            raise ValueError("булево вместо числа")
        number = int(value)
        if spec is not None:
            if spec.minimum is not None:
                number = max(spec.minimum, number)
            if spec.maximum is not None:
                number = min(spec.maximum, number)
        return number
    if kind == "choice":
        text = str(value).strip().lower()
        if spec is not None and spec.choices and text not in spec.choices:
            raise ValueError(f"недопустимый выбор: {value!r}")
        return text
    if kind == "strlist":
        if isinstance(value, str):
            items = value.replace(";", ",").replace("\n", ",").split(",")
        elif isinstance(value, (list, tuple)):
            items = list(value)
        else:
            raise ValueError("ожидается список строк")
        result = [str(item).strip() for item in items if str(item).strip()]
        if not result:
            raise ValueError("пустой список")
        return result
    return str(value)


def _coerce_like_default(default: Any, value: Any) -> Any:
    """Приводит значение из settings.json к типу умолчания поля.

    Для параметров, которых нет в меню (max_configs_per_source,
    yaml_node_limit и т.п.), реестра ParamSpec нет — тип берётся по
    значению по умолчанию: bool/int/float/str. Без этого целые поля
    загружались строками и ломали парсер (сравнение числа со строкой
    валило разбор каждого источника).
    """
    if isinstance(default, bool):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in _TRUE_WORDS:
                return True
            if text in _FALSE_WORDS:
                return False
        raise ValueError(f"некорректное булево значение: {value!r}")
    if isinstance(default, int):
        if isinstance(value, bool):
            raise ValueError("булево вместо числа")
        return int(value)
    if isinstance(default, float):
        if isinstance(value, bool):
            raise ValueError("булево вместо числа")
        return float(value)
    return str(value)


def load_settings(path: Path) -> Settings:
    """Читает settings.json (utf-8-sig, устойчиво к BOM); ошибки игнорируются.

    Чего в файле не хватает — берётся из значений по умолчанию, так что
    старый settings.json спокойно живёт в новом коде: новые ключи просто
    появятся при ближайшем сохранении. Параметры из меню приводятся по
    своим описаниям (с мин/макс), остальные — по типу поля в Settings.
    """
    settings = Settings()
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return settings
    if not isinstance(data, dict):
        return settings
    for f in fields(Settings):
        if f.name not in data:
            continue
        spec = ALL_PARAMS.get(f.name)
        try:
            if spec is not None:
                setattr(settings, f.name, coerce_value(spec, data[f.name]))
            else:
                # параметра нет в меню — приводим по типу умолчания
                setattr(settings, f.name,
                        _coerce_like_default(getattr(settings, f.name), data[f.name]))
        except (TypeError, ValueError):
            pass  # некорректное значение — оставляем значение по умолчанию
    return settings


def retune_legacy_data(data: dict) -> list[tuple[str, int, int]]:
    """Подгоняет settings.json от прошлых версий под умолчания этой версии.

    Правило простое: если параметр равен одному из СТАРЫХ умолчаний,
    считается, что пользователь его не менял, — и он переезжает на новое
    значение. Иначе старый файл молча держал бы прежние числа (12 потоков
    загрузки, лимит ответа 512 МБ, пять раундов добора), хотя код уже
    настроен иначе.
    Значения, выставленные вручную (не равные старым умолчаниям), не
    трогаются. Возвращается список правок (ключ, старое, новое) — их
    применяет и показывает App, после чего файл перезаписывается, и при
    следующих запусках подгонка больше не срабатывает.
    """
    if not isinstance(data, dict):
        return []
    notes: list[tuple[str, int, int]] = []
    # ключ → (новое значение, старые умолчания прошлых версий)
    migration: dict[str, tuple[int, tuple[int, ...]]] = {
        "http_threads": (30, (12,)),
        "github_fix_threads": (15, (6, 12)),
        "max_response_mb": (128, (512, 96)),
        "base64_max_len_mb": (32, (64,)),
        "json_yaml_max_len_mb": (32, (64,)),
        "conveyor_drain_max_rounds": (1, (5,)),
    }
    for key, (new, olds) in migration.items():
        old = data.get(key)
        if old in olds:
            data[key] = new
            notes.append((key, old, new))
    return notes


def save_settings(path: Path, settings: Settings) -> None:
    """Сохраняет настройки в JSON — той же устойчивой записью, что и всё остальное."""
    atomic_write_text(path, json.dumps(settings.to_dict(), ensure_ascii=False, indent=2))


def copy_defaults(target: Settings) -> None:
    """Сбрасывает все поля target к значениям по умолчанию (объект сохраняет идентичность)."""
    fresh = Settings()
    for f in fields(Settings):
        setattr(target, f.name, getattr(fresh, f.name))


# ══════════════════════════════════════════════════════════════════════════════
# ДЛИТЕЛЬНОСТИ: показ по-человечески («2 часа») и разбор ввода с суффиксами
# ══════════════════════════════════════════════════════════════════════════════

def _ru_plural(n: int, one: str, few: str, many: str) -> str:
    """Русская плюрализация: 1 час / 2 часа / 5 часов."""
    n = abs(n)
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def fmt_duration(seconds: int) -> str:
    """«2 часа», «45 минут», «30 сек» — сколько секунд, так и сказано.

    Долгие промежутки (отдых, кулдаун ЧС) показываются часами и минутами,
    коротким таймаутам честно остаются секунды: «дедлайн 0.008 часа»
    читать невозможно.
    """
    seconds = max(0, int(seconds))
    if seconds >= 3600:
        hours, rem = divmod(seconds, 3600)
        text = f"{hours} {_ru_plural(hours, 'час', 'часа', 'часов')}"
        if rem >= 60:
            text += f" {rem // 60} {_ru_plural(rem // 60, 'мин', 'мин', 'мин')}"
        return text
    if seconds >= 60:
        minutes, sec = divmod(seconds, 60)
        text = f"{minutes} {_ru_plural(minutes, 'минуту', 'минуты', 'минут')}"
        if sec:
            text += f" {sec} сек"
        return text
    return f"{seconds} сек"


def fmt_hours(hours: int) -> str:
    """«24 часа» — для параметров, которые задаются сразу в часах."""
    hours = max(0, int(hours))
    return f"{hours} {_ru_plural(hours, 'час', 'часа', 'часов')}"


_DUR_TOKEN_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(ч|час[а-яё]*|h|м|мин[а-яё]*|m|с|сек[а-яё]*|s)")


def parse_duration_input(raw: str) -> int | None:
    """Разбирает ввод длительности и возвращает секунды (None — не поняли).

    Голое число — секунды (как в старом интерфейсе), с суффиксом — как
    написано: «2ч», «1.5h», «90м», «1ч30м», «45с». Буквы годятся и русские,
    и латинские — с английской раскладки «2ч» не набрать, а «2h» сколько
    угодно.
    """
    text = raw.strip().lower().replace(",", ".")
    if not text:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return int(round(float(text)))
    total = 0.0
    matched = False
    for num, unit in _DUR_TOKEN_RE.findall(text):
        matched = True
        n = float(num)
        if unit.startswith(("ч", "h")):
            total += n * 3600
        elif unit.startswith(("м", "m")):
            total += n * 60
        else:
            total += n
    return int(round(total)) if matched else None


# ══════════════════════════════════════════════════════════════════════════════
# ФАЙЛОВЫЕ УТИЛИТЫ И ПАМЯТЬ
# ══════════════════════════════════════════════════════════════════════════════

_URL_LINE_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)


def is_url_line(line: str) -> bool:
    """Строка является HTTP/HTTPS-ссылкой."""
    return _URL_LINE_RE.match(clean_source_line(line)) is not None


def _short_url(url: str, limit: int = 64) -> str:
    """URL для вывода в консоль: обрезается до limit символов."""
    return url if len(url) <= limit else url[: limit - 1] + "…"


def read_lines_safe(path: Path) -> list[str]:
    """Читает строки файла (utf-8-sig); при ошибке возвращает пустой список.

    Использовать только для небольших файлов (sources.txt и т.п.).
    """
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except OSError:
        return []


def iter_file_lines(path: Path) -> Iterator[str]:
    """Построчное чтение файла (не загружает файл целиком в память)."""
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                yield line.rstrip("\r\n")
    except OSError:
        return


def write_lines(path: Path, lines: list[str]) -> None:
    """Атомарно перезаписывает файл списком строк."""
    text = "\n".join(line.rstrip("\r\n") for line in lines)
    atomic_write_text(path, text + ("\n" if text else ""))


def atomic_write_text(path: Path, text: str, attempts: int = 4) -> None:
    """Запись во временный файл в той же папке + os.replace (атомарность).

    На Windows os.replace капризничает, пока файл прикрыт чужим процессом:
    антивирус и OneDrive любят подержать свежезаписанный файл, а собственные
    читатели не выставляют FILE_SHARE_DELETE — без этого replace отвечает
    «Отказано в доступе» именно в момент, когда запись нужна больше всего.
    Поэтому: пара вежливых повторов с паузой, а если и они не пробили —
    обычная перезапись поверх, которая проходит там, где упёрся replace.
    Файл цел, данные на месте.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent),
                                        prefix=path.name + ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.15 * (attempt + 1))
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    except OSError:
        raise last_error if last_error else OSError(f"не удалось записать {path}")


class _AdvisoryLock:
    """Мягкий advisory-замок файлом-маркером (контекстный менеджер).

    sources.txt пишут два процесса: этот (перезапись) и Telegram_Checker/
    telegram_sub_bot (дозапись). Дозапись, вклинившаяся между нашим
    чтением и os.replace, на Windows теряется — дескриптор остаётся на
    старом файле. Захват замка (O_CREAT|O_EXCL) перед переписыванием — и
    такой же замок у соседей — сужает окно гонки. Замок мягкий: не
    дождались за timeout — предупреждаем и работаем без него; зависший
    замок соседа не останавливает конвейер.
    """

    def __init__(self, path: Path, timeout: float = 15.0, poll: float = 0.1,
                 console: Any = None, name: str = "") -> None:
        self.path = path
        self.timeout = timeout
        self.poll = poll
        self.console = console
        self.name = name or path.name

    def _warn(self, message: str) -> None:
        if self.console is not None:
            try:
                self.console.warn(message)
                return
            except Exception:
                pass
        print(f"[!] {message}", flush=True)

    def __enter__(self) -> "_AdvisoryLock":
        deadline = time.monotonic() + self.timeout
        fd: int | None = None
        while True:
            try:
                fd = os.open(str(self.path),
                             os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileExistsError:
                if time.monotonic() >= deadline:
                    self._warn(f"{self.name}: файл-замок держит другой процесс дольше "
                               f"{self.timeout:.0f} сек — продолжаю без блокировки "
                               f"(если это зависший замок, файл {self.path.name} можно удалить)")
                    break
                time.sleep(self.poll)
            except OSError:
                break    # замки здесь не работают — живём без них
        if fd is not None:
            try:
                os.write(fd, str(os.getpid()).encode("ascii"))
            except OSError:
                pass
            finally:
                os.close(fd)
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            pass


try:
    import psutil as _psutil
    _psutil_process = _psutil.Process()
except Exception:  # psutil недоступен — память не показываем
    _psutil_process = None


def get_memory_mb() -> float | None:
    if _psutil_process is None:
        return None
    try:
        return _psutil_process.memory_info().rss / (1024 * 1024)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# ОТЛАДКА: подробный журнал в logs/Downloader (включается настройкой debug_logs)
# ══════════════════════════════════════════════════════════════════════════════

class DebugLog:
    """Подробный журнал каждого запуска — файл в logs/Downloader.

    В консоль такой объём не выведешь (засорит экран), поэтому журнал
    живёт в файле logs/Downloader/run_ГГГГ-ММ-ДД_ЧЧ-ММ-СС.log, по одному
    на запуск. Что туда попадает:
      • по каждому источнику — статус HTTP, размер ответа, время;
      • каким методом добыты конфиги: прямые ссылки, каждый уровень
        Base64, JSON, YAML — с количеством;
      • вердикт по источнику: сколько найдено/записано, через Happ ли;
      • каждое решение по чёрному списку — с причиной: отправлен,
        отложен по кулдауну, реабилитирован;
      • дедлайны: кто превысил и что за него стало — с конфигами или без.

    Пишется потокобезопасно, одна строка за раз, с меткой времени.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fh = None

    @property
    def enabled(self) -> bool:
        return self._fh is not None

    def enable(self) -> None:
        """Открывает файл журнала; не получилось (права, диск) — живём без него."""
        with self._lock:
            if self._fh is not None:
                return
            try:
                DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                name = time.strftime("run_%Y-%m-%d_%H-%M-%S.log")
                self._fh = open(DEBUG_DIR / name, "a", encoding="utf-8", buffering=1)
                self._write("СТАРТ", f"журнал открыт: {name}")
            except OSError:
                self._fh = None

    def disable(self) -> None:
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.close()
                except OSError:
                    pass
                self._fh = None

    def log(self, tag: str, text: str) -> None:
        with self._lock:
            if self._fh is not None:
                self._write(tag, text)

    def raw(self, text: str) -> None:
        """Строка без метки и тега — для дампов настроек и итогов."""
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.write(text + "\n")
                except OSError:
                    pass

    def _write(self, tag: str, text: str) -> None:  # вызывается под замком
        try:
            self._fh.write(f"[{time.strftime('%H:%M:%S')}] [{tag}] {text}\n")
        except OSError:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# КУЛДАУН ЧЁРНОГО СПИСОКА: помним, когда источник последний раз уезжал в ЧС
# ══════════════════════════════════════════════════════════════════════════════

class BlacklistBook:
    """Память «когда кого отправляли» для метода «Кулдаун».

    Метод «не чаще раза в N часов» требует помнить решения между
    запусками — иначе после перезапуска программа забудет, кого недавно
    уже отправляла в ЧС, и начнёт дублировать их каждый цикл. Таблица
    {url: время отправки} лежит в temp/Downloader/blacklist_cooldown.txt —
    построчный текст «url<TAB>время» рядом с настройками — и подрезается:
    записи старше двух недель чистятся, файл не разбухает. Старый
    blacklist_cooldown.json конвертируется при первой загрузке.
    """
    KEEP_DAYS: int = 14  # сколько дней хранить метки

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._marks: dict[str, float] = {}
        self._loaded = False
        self._dirty = False
        self._last_save = 0.0

    def _ensure_loaded(self) -> None:  # вызывается под замком
        if self._loaded:
            return
        self._loaded = True
        # Текущий формат: построчный текст «url<TAB>время»
        try:
            for line in self.path.read_text(encoding="utf-8-sig").splitlines():
                line = line.strip()
                if not line:
                    continue
                url, sep, ts = line.rpartition("\t")  # URL не содержит табуляций
                if not sep:
                    continue
                try:
                    self._marks[url] = float(ts)
                except ValueError:
                    continue
        except OSError:
            pass
        # Легаси: JSON-файл кулдауна от прошлой версии — сливаем и удаляем.
        # Работает и когда .txt ещё нет (обычный переход на новую версию),
        # и когда оба файла существуют (вдруг .txt успел появиться раньше).
        legacy = self.path.with_suffix(".json")
        if legacy.exists():
            try:
                data = json.loads(legacy.read_text(encoding="utf-8-sig"))
            except (OSError, ValueError):
                data = None
            if isinstance(data, dict):
                merged = 0
                for key, ts in data.items():
                    if isinstance(ts, (int, float)) and isinstance(key, str):
                        self._marks[key] = float(ts)
                        merged += 1
                if merged:
                    self._dirty = True
                    self._save_locked()
            try:
                legacy.unlink()
            except OSError:
                pass

    def _save_locked(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            text = "".join(f"{url}\t{ts:.0f}\n" for url, ts in self._marks.items())
            atomic_write_text(self.path, text)
            self._dirty = False
        except OSError:
            pass  # не сохранили — пометка останется, уедет на диск при следующем flush

    def _auto_flush_locked(self) -> None:  # вызывается под замком
        """Пишет на диск не чаще раза в 15 сек, даже если главный поток спит.

        Фоновые воркеры, дорабатывающие после конца цикла, ставят метки
        уже после того, как цикл сделал свой финальный сброс — без этой
        страховки метки дожили бы до следующего цикла или выхода.
        """
        if time.monotonic() - self._last_save >= 15.0:
            self._last_save = time.monotonic()
            self._save_locked()

    def on_cooldown(self, url: str, hours: int) -> bool:
        """Истина, если источник отправляли в ЧС меньше, чем hours назад."""
        with self._lock:
            self._ensure_loaded()
            ts = self._marks.get(url)
            return ts is not None and (time.time() - ts) < max(1, hours) * 3600

    def mark(self, url: str) -> None:
        """Запоминает (или обновляет) время отправки источника в ЧС.

        Только в памяти: файл с десятками тысяч меток перезаписывать после
        каждой смерти источника — значит нагружать диск без нужды и
        напрашиваться на «Отказано в доступе» от антивируса. На диск
        таблица попадает через maybe_flush()/flush() — по расписанию,
        а не на каждый чих.
        """
        with self._lock:
            self._ensure_loaded()
            self._marks[url] = time.time()
            horizon = time.time() - self.KEEP_DAYS * 86400
            self._marks = {k: v for k, v in self._marks.items() if v >= horizon}
            self._dirty = True
            self._auto_flush_locked()

    def clear(self, url: str) -> None:
        """Источник реабилитирован — метка о нём больше не нужна."""
        with self._lock:
            self._ensure_loaded()
            if url in self._marks:
                del self._marks[url]
                self._dirty = True
                self._auto_flush_locked()

    def maybe_flush(self, interval: float = 15.0) -> None:
        """Сбрасывает метки на диск, если с прошлой записи прошло больше interval секунд."""
        with self._lock:
            if not self._dirty or time.monotonic() - self._last_save < interval:
                return
            self._last_save = time.monotonic()
            self._save_locked()

    def flush(self) -> None:
        """Сбрасывает метки на диск немедленно (конец цикла, выход из программы)."""
        with self._lock:
            if self._dirty:
                self._last_save = time.monotonic()
                self._save_locked()


# ══════════════════════════════════════════════════════════════════════════════
# ПАРСЕР: извлечение, валидация, нормализация, Base64/JSON/YAML (потоково)
# ══════════════════════════════════════════════════════════════════════════════

CANONICAL_SCHEMES: tuple[str, ...] = ("vless", "trojan", "ss", "vmess", "hysteria2", "tuic")
ALIAS_TO_CANONICAL: dict[str, str] = {"shadowsocks": "ss", "hy2": "hysteria2"}
_ALL_SCHEMES: str = "|".join((*CANONICAL_SCHEMES, *ALIAS_TO_CANONICAL))

CONFIG_URL_RE: re.Pattern[str] = re.compile(
    rf"(?<![A-Za-z0-9+.\-])(?:{_ALL_SCHEMES})://[^\s<>\"']+", re.IGNORECASE
)
_SCHEME_PREFIX_RE: re.Pattern[str] = re.compile(rf"^(?:{_ALL_SCHEMES})://", re.IGNORECASE)
# Граница схемы внутри «склеенного» блоба (два конфига без разделителя).
# Lookbehind отсекает ложные границы ВНУТРИ слова схемы («ve|ss://»,
# «vmе|ss://»), но разрешает разрез после пунктуации-разделителя («,» «.» «;» ...)
_SCHEME_BOUNDARY_RE: re.Pattern[str] = re.compile(
    rf"(?<![A-Za-z0-9+\-])(?=(?:{_ALL_SCHEMES})://)", re.IGNORECASE
)

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_SECRET_RE = re.compile(r"^[A-Za-z0-9_.%+-]{1,256}$")
_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")
_IPV4_RE = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")
_B64_CHARSET_RE = re.compile(r"^[A-Za-z0-9+/\-_\s=]+$")
_HTML_RE = re.compile(r"<\s*(!doctype|html|body|head)\b", re.IGNORECASE)

_TRAILING_JUNK: str = "\"'`>,;)]."  # «.» — точка в конце предложения
_MB: int = 1024 * 1024


# ---------- base64 ------------------------------------------------------------

def _b64_decode(data: str) -> bytes | None:
    """Base64-декод с автодополнением паддинга и поддержкой urlsafe-алфавита."""
    core = re.sub(r"\s+", "", data)
    if not core:
        return None
    pad = "=" * (-len(core) % 4)
    for candidate in (core + pad, core.replace("-", "+").replace("_", "/") + pad):
        try:
            return base64.b64decode(candidate, validate=False)
        except (ValueError, TypeError):
            continue
    return None


def _printable_ratio(text: str, sample: int = 8192) -> float:
    """Доля печатных символов в выборке текста (начало + середина)."""
    if not text:
        return 0.0
    seg = text if len(text) <= sample else text[: sample // 2] + text[len(text) // 2: len(text) // 2 + sample // 2]
    good = sum(1 for ch in seg if ch.isprintable() or ch in "\r\n\t")
    return good / len(seg)


def _b64_to_text(data: str) -> str | None:
    """Base64 → текст UTF-8; мусор (малопечатный результат) — None."""
    raw = _b64_decode(data)
    if raw is None:
        return None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return text if _printable_ratio(text) >= 0.95 else None


def _looks_like_base64(text: str, max_chars: int) -> bool:
    """Похоже на Base64-блок: подходящая длина и алфавит."""
    core = text.strip()
    if len(core) < 24 or len(core) > max_chars:
        return False
    if not _B64_CHARSET_RE.fullmatch(core):
        return False
    compact = len(re.sub(r"\s+", "", core))
    return compact >= 24 and compact % 4 != 1


def _std_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def looks_like_html(text: str) -> bool:
    return bool(_HTML_RE.search(text[:4096]))


# ---------- host:port ---------------------------------------------------------

def _valid_host(host: str) -> bool:
    """IPv4, IPv6 или корректное доменное имя."""
    if not host:
        return False
    if _IPV4_RE.match(host):
        return all(0 <= int(octet) <= 255 for octet in host.split("."))
    if ":" in host:  # IPv6 (скобки сняты вызывающим кодом) — проверяем как адрес
        try:
            ipaddress.IPv6Address(host)
        except ValueError:
            return False
        return True
    return _HOSTNAME_RE.match(host) is not None


def _split_hostport(value: str) -> tuple[str, int] | None:
    """Разбор 'host:port' / '[ipv6]:port'; возвращает (host_repr, port)."""
    value = value.strip()
    if not value or "/" in value:
        return None
    if value.startswith("["):
        end = value.find("]")
        if end == -1:
            return None
        host_repr, host = value[: end + 1], value[1:end]
        rest = value[end + 1:]
        if not rest.startswith(":"):
            return None
        port_s = rest[1:]
    else:
        host_repr, sep, port_s = value.rpartition(":")
        # «Голый» IPv6 без скобок неоднозначен (нельзя отделить порт) — отклоняем
        if not sep or ":" in host_repr:
            return None
        host = host_repr
    if not port_s.isdigit():
        return None
    port = int(port_s)
    if not 1 <= port <= 65535 or not _valid_host(host):
        return None
    return host_repr, port


def _canonical_hostport(value: str) -> str:
    """host:port в нижнем регистре; не host:port — возвращается как есть."""
    parsed = _split_hostport(value)
    if parsed is None:
        return value  # не host:port (например, base64 у vmess) — не трогаем
    host_repr, port = parsed
    return f"{host_repr.lower()}:{port}"


def _fix_netloc_case(netloc: str) -> str:
    """Нижний регистр только у host (userinfo/base64 не трогаем) — стабильный дедупликационный хэш."""
    if "@" in netloc:
        userinfo, _, hostport = netloc.rpartition("@")
        return f"{userinfo}@{_canonical_hostport(hostport)}"
    return _canonical_hostport(netloc)


def _host_repr(host: str) -> str:
    host = host.strip("[]")
    return f"[{host}]" if ":" in host else host


# ---------- валидация ---------------------------------------------------------

def _check_user_host(netloc: str, *, need_user: bool) -> bool:
    """Проверка схем вида user@host:port (vless/vmess/trojan/hysteria2).

    Короткие UUID (1–7 символов) — мусор для vless/vmess. Короткие пароли
    trojan/hysteria2 — легитимны, их не отбраковываем.
    """
    if "@" in netloc:
        userinfo, _, hostport = netloc.rpartition("@")
        if not hostport or not userinfo:
            return False
        if need_user and len(userinfo) < 8:
            return False
        if not (_UUID_RE.match(userinfo) or _SECRET_RE.match(userinfo)):
            return False
    else:
        if need_user:
            return False
        hostport = netloc
    return _split_hostport(hostport) is not None


def _check_ss(netloc: str, path: str) -> bool:
    """SIP002 (user@host:port) или legacy (ss://base64(method:pass@host:port))."""
    if "@" in netloc:
        userinfo, _, hostport = netloc.rpartition("@")
        if not userinfo or " " in userinfo:
            return False
        return _split_hostport(hostport) is not None
    blob = (netloc + path).strip("/")  # legacy: ss://base64(method:pass@host:port)
    if not blob:
        return False
    raw = _b64_decode(blob)
    if raw is None:
        return False
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return False
    if "@" not in text:
        return False
    cred, _, hostport = text.rpartition("@")
    if ":" not in cred:
        return False
    return _split_hostport(hostport.strip()) is not None


def _check_vmess(netloc: str, path: str) -> bool:
    """Новый формат vmess://uuid@host:port?... или legacy vmess://base64(JSON)."""
    if "@" in netloc:
        return _check_user_host(netloc, need_user=True)
    # Legacy: vmess://base64(JSON). Payload с '/' urlsplit уносит в path,
    # поэтому склеиваем netloc и path обратно (как в _check_ss). Хвостовой
    # «украшающий» слэш декодирование ломает — пробуем и вариант без него.
    blob = netloc + path
    for candidate in dict.fromkeys((blob, blob.rstrip("/"))):
        if _vmess_payload_ok(candidate):
            return True
    return False


def _check_tuic(netloc: str) -> bool:
    """tuic://uuid:password@host:port — v5-формат с двумя частями учётки.

    Userinfo обязан содержать ':' (v4-токен без пароля ядро и клиенты не
    принимают), uuid — валидный UUID или секретный токен, пароль может
    быть пустым (некоторые серверы проверяют только uuid)."""
    if "@" not in netloc:
        return False
    userinfo, _, hostport = netloc.rpartition("@")
    if not userinfo or not hostport:
        return False
    uuid_part, sep, pwd_part = userinfo.partition(":")
    if not sep:
        return False
    if not (_UUID_RE.match(uuid_part) or _SECRET_RE.match(uuid_part)):
        return False
    if len(pwd_part) > 256 or " " in pwd_part:
        return False
    return _split_hostport(hostport) is not None


def _vmess_payload_ok(blob: str) -> bool:
    """Проверка legacy-vmess: base64 декодируется в JSON с валидными add/port/id."""
    raw = _b64_decode(blob)
    if raw is None:
        return False
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return False
    if not isinstance(obj, dict):
        return False
    add, port, uid = obj.get("add"), obj.get("port"), obj.get("id")
    if not isinstance(add, str) or not add.strip():
        return False
    if isinstance(port, bool) or not (isinstance(port, int) or (isinstance(port, str) and port.isdigit())):
        return False
    if not isinstance(uid, str) or len(uid) < 8 or not (_UUID_RE.match(uid) or _SECRET_RE.match(uid)):
        return False
    add_s = add.strip()
    if ":" in add_s:  # IPv6-адрес (в vmess-JSON указывается без скобок)
        try:
            ipaddress.IPv6Address(add_s)
        except ValueError:
            return False
        return 1 <= int(port) <= 65535
    return _split_hostport(f"{add_s}:{port}") is not None


def _validate_parts(scheme: str, netloc: str, path: str) -> bool:
    try:
        if scheme in ALIAS_TO_CANONICAL:
            return _validate_parts(ALIAS_TO_CANONICAL[scheme], netloc, path)
        if scheme == "vless":
            return _check_user_host(netloc, need_user=True)
        if scheme == "trojan":
            return _check_user_host(netloc, need_user=False)
        if scheme == "hysteria2":
            return _check_user_host(netloc, need_user=False)
        if scheme == "ss":
            return _check_ss(netloc, path)
        if scheme == "vmess":
            return _check_vmess(netloc, path)
        if scheme == "tuic":
            return _check_tuic(netloc)
    except Exception:
        return False
    return False


# ---------- нормализация ------------------------------------------------------

def normalize_config_url(raw: str, settings: Settings) -> str | None:
    """Валидирует URL-конфиг и приводит к каноничному виду.

    Убирает #фрагмент, сортирует query-параметры, приводит алиасы схем,
    ограничивает длину и число параметров. Возвращает None для невалидных.
    """
    s = raw.strip().strip("\"'`").rstrip(_TRAILING_JUNK)
    if not _SCHEME_PREFIX_RE.match(s):
        return None
    try:
        parts = urlsplit(s)
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme in ALIAS_TO_CANONICAL and settings.normalize_ss_hy2_aliases:
        scheme = ALIAS_TO_CANONICAL[scheme]
    if scheme not in CANONICAL_SCHEMES and scheme not in ALIAS_TO_CANONICAL:
        return None
    netloc = _fix_netloc_case(parts.netloc)
    if not _validate_parts(scheme, netloc, parts.path):
        return None
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    # Сначала сортировка, потом обрезка — иначе состав сохранённых параметров
    # зависит от порядка в исходной строке и может потерять важные параметры
    pairs.sort()
    if len(pairs) > settings.max_query_params:
        pairs = pairs[: settings.max_query_params]
    url = urlunsplit((scheme, netloc, parts.path, urlencode(pairs, safe=""), ""))
    if len(url) > settings.max_url_length:
        return None
    return url


def _split_glued(blob: str) -> list[str]:
    """Развязывает склеенные конфиги («vmess://AAA,vless://BBB») на части.

    Границами считаются позиции scheme:// строго ПОСЛЕ ведущей схемы —
    иначе «vless://» разрезался бы по внутреннему «ss://» (ve|ss://).
    """
    head = _SCHEME_PREFIX_RE.match(blob)
    if head is None:
        return [blob]
    bounds = [m.start() for m in _SCHEME_BOUNDARY_RE.finditer(blob)
              if m.start() >= head.end()]
    if not bounds:
        return [blob]
    parts, prev = [], 0
    for pos in [*bounds, len(blob)]:
        if pos > prev:
            parts.append(blob[prev:pos])
        prev = pos
    return parts


def iter_extract_config_urls(text: str, settings: Settings) -> Iterator[str]:
    """Итератор нормализованных URL-конфигов из текста (без списка в памяти)."""
    if not text:
        return
    count = 0
    limit = settings.max_configs_per_source
    for match in CONFIG_URL_RE.finditer(text):
        if count >= limit:
            return
        blob = match.group(0)
        # Защита от «склейки»: если внутри match есть граница другой схемы
        # («...vmess://AAA,vless://BBB...»), цельный URL даже не пробуем —
        # снисходительный base64-декод может «проглотить» хвост склейки.
        chunks = _split_glued(blob)
        if len(chunks) == 1:
            normalized = normalize_config_url(blob, settings)
            if normalized:
                count += 1
                yield normalized
            continue
        for chunk in chunks:
            if count >= limit:
                return
            normalized = normalize_config_url(chunk, settings)
            if normalized:
                count += 1
                yield normalized


# ---------- JSON / YAML -------------------------------------------------------

class _LimitedSafeLoader(yaml.SafeLoader):
    """SafeLoader с лимитом числа узлов (защита от YAML-бомб)."""
    node_limit: int = 100_000

    def __init__(self, stream: Any) -> None:
        super().__init__(stream)
        self._count = 0

    def construct_object(self, node: yaml.Node, deep: bool = False):  # type: ignore[override]
        self._count += 1
        if self._count > self.node_limit:
            raise ValueError("превышен лимит узлов YAML")
        return super().construct_object(node, deep)


def _yaml_safe_load_limited(text: str, limit: int) -> Any:
    cls = type("LimitedLoader", (_LimitedSafeLoader,), {"node_limit": limit})
    return yaml.load(text, Loader=cls)


def _addr_port(srv: Any) -> tuple[str, int] | None:
    """Достаёт (адрес, порт) из узла Clash/sing-box/Xray; None — данные кривые."""
    if not isinstance(srv, dict):
        return None
    addr = srv.get("address") or srv.get("server")
    port = srv.get("port") or srv.get("server_port")
    if not isinstance(addr, str) or not addr.strip():
        return None
    if isinstance(port, bool):
        return None
    if isinstance(port, float) and port.is_integer():
        port = int(port)
    if isinstance(port, str) and port.strip().isdigit():
        port = int(port.strip())
    if not isinstance(port, int) or not 1 <= port <= 65535:
        return None
    return addr.strip(), port


def _tls_info(p: dict[str, Any]) -> tuple[str, str, str, str, str, bool]:
    """Унификация TLS-полей Clash и sing-box → (security, sni, fp, pbk, sid, insecure)."""
    tls = p.get("tls")
    reality_opts = p.get("reality-opts") or {}
    sni = fp = pbk = sid = ""
    insecure = False
    if isinstance(tls, dict):  # sing-box
        reality = tls.get("reality") if isinstance(tls.get("reality"), dict) else {}
        if tls.get("enabled") and reality.get("enabled"):
            security = "reality"
        elif tls.get("enabled"):
            security = "tls"
        else:
            security = ""
        sni = str(tls.get("server_name") or "")
        utls = tls.get("utls") if isinstance(tls.get("utls"), dict) else {}
        fp = str(utls.get("fingerprint") or "")
        pbk = str(reality.get("public_key") or "")
        sid = str(reality.get("short_id") or "")
        insecure = bool(tls.get("insecure"))
    else:  # Clash
        enabled = bool(tls) or bool(reality_opts)
        security = "reality" if reality_opts else ("tls" if enabled else "")
        sni = str(p.get("servername") or p.get("sni") or "")
        fp = str(p.get("client-fingerprint") or "")
        pbk = str(reality_opts.get("public-key") or reality_opts.get("public_key") or "")
        sid = str(reality_opts.get("short-id") or reality_opts.get("short_id") or "")
        insecure = bool(p.get("skip-cert-verify"))
    if not fp:
        fp = str(p.get("client-fingerprint") or "")
    return security, sni, fp, pbk, sid, insecure


def _transport_into_q(p: dict[str, Any], q: dict[str, str]) -> None:
    network = str(p.get("network") or "").lower()
    transport = p.get("transport") if isinstance(p.get("transport"), dict) else {}
    net = network or str(transport.get("type") or "").lower()
    if net in ("", "tcp", "none"):
        return
    q["type"] = net
    ws_opts = p.get("ws-opts") if isinstance(p.get("ws-opts"), dict) else {}
    grpc_opts = p.get("grpc-opts") if isinstance(p.get("grpc-opts"), dict) else {}
    tr_headers = transport.get("headers") if isinstance(transport.get("headers"), dict) else {}
    ws_headers = ws_opts.get("headers") if isinstance(ws_opts.get("headers"), dict) else {}
    host_hdr = (ws_headers.get("Host") or ws_headers.get("host")
                or tr_headers.get("Host") or tr_headers.get("host") or "")
    if net in ("ws", "httpupgrade", "h2"):
        q["path"] = str(ws_opts.get("path") or transport.get("path") or "/")
        if host_hdr:
            q["host"] = str(host_hdr)
    elif net == "grpc":
        svc = grpc_opts.get("grpc-service-name") or transport.get("service_name") or ""
        if svc:
            q["serviceName"] = str(svc)


def _vless_from_proxy(p: dict[str, Any]) -> list[str]:
    """Clash/sing-box vless-прокси → vless:// URL."""
    hp = _addr_port(p)
    uuid = p.get("uuid") or p.get("id")
    if isinstance(uuid, (int, float)) and not isinstance(uuid, bool):
        uuid = str(uuid)  # YAML: uuid: 12345678 — число, а не строка
    if hp is None or not isinstance(uuid, str) or len(uuid) < 8 \
            or not (_UUID_RE.match(uuid) or _SECRET_RE.match(uuid)):
        return []
    host, port = hp
    q: dict[str, str] = {"encryption": str(p.get("encryption") or "none")}
    if p.get("flow"):
        q["flow"] = str(p["flow"])
    security, sni, fp, pbk, sid, insecure = _tls_info(p)
    if security == "reality":
        q["security"] = "reality"
        if sni: q["sni"] = sni
        if fp: q["fp"] = fp
        if pbk: q["pbk"] = pbk
        if sid: q["sid"] = sid
    elif security == "tls":
        q["security"] = "tls"
        if sni: q["sni"] = sni
        if fp: q["fp"] = fp
        if insecure: q["allowInsecure"] = "1"
    _transport_into_q(p, q)
    query = urlencode(sorted(q.items()), safe="")
    return [f"vless://{quote(uuid, safe='')}@{_host_repr(host)}:{port}?{query}"]


def _trojan_from_proxy(p: dict[str, Any]) -> list[str]:
    """Clash/sing-box trojan-прокси → trojan:// URL."""
    hp = _addr_port(p)
    password = p.get("password") or p.get("uuid") or ""
    if isinstance(password, (int, float)) and not isinstance(password, bool):
        password = str(password)  # YAML: password: 123456 — число, а не строка
    if hp is None or not isinstance(password, str) or not password:
        return []
    host, port = hp
    q: dict[str, str] = {}
    security, sni, fp, pbk, sid, insecure = _tls_info(p)
    if sni: q["sni"] = sni
    if fp: q["fp"] = fp
    if security == "reality" and pbk:
        q["security"] = "reality"
        q["pbk"] = pbk
        if sid: q["sid"] = sid
    if insecure:
        q["allowInsecure"] = "1"
    _transport_into_q(p, q)
    query = ("?" + urlencode(sorted(q.items()), safe="")) if q else ""
    return [f"trojan://{quote(password, safe='')}@{_host_repr(host)}:{port}{query}"]


def _ss_from_proxy(p: dict[str, Any]) -> list[str]:
    """Clash/sing-box shadowsocks-прокси → ss:// URL (SIP002, userinfo в base64)."""
    hp = _addr_port(p)
    method = str(p.get("cipher") or p.get("method") or "")
    password = p.get("password")
    if isinstance(password, (int, float)) and not isinstance(password, bool):
        password = str(password)  # YAML: password: 123456 — число, а не строка
    if hp is None or not method or not isinstance(password, str) or not password:
        return []
    host, port = hp
    userinfo = _b64_no_pad(f"{method}:{password}".encode("utf-8"))
    q: dict[str, str] = {}
    plugin = p.get("plugin")
    if isinstance(plugin, str) and plugin:
        plugin_opts = p.get("plugin_opts") if "plugin_opts" in p else p.get("plugin-opts")
        if isinstance(plugin_opts, dict):
            value = plugin
            mode = str(plugin_opts.get("mode", ""))
            host_o = str(plugin_opts.get("host", ""))
            if mode:
                value += ";obfs=" + mode
            if host_o:
                value += ";obfs-host=" + host_o
        elif isinstance(plugin_opts, str) and plugin_opts:
            value = f"{plugin};{plugin_opts}"
        else:
            value = plugin
        q["plugin"] = value
    query = ("?" + urlencode(sorted(q.items()), safe="")) if q else ""
    return [f"ss://{userinfo}@{_host_repr(host)}:{port}{query}"]


def _vmess_obj(p: dict[str, Any], host: str, port: int, uuid: str) -> dict[str, Any]:
    security, sni, fp, _pbk, _sid, _insecure = _tls_info(p)
    transport = p.get("transport") if isinstance(p.get("transport"), dict) else {}
    ws_opts = p.get("ws-opts") if isinstance(p.get("ws-opts"), dict) else {}
    net = (str(p.get("network") or "") or str(transport.get("type") or "") or "tcp").lower()
    ws_headers = ws_opts.get("headers") if isinstance(ws_opts.get("headers"), dict) else {}
    tr_headers = transport.get("headers") if isinstance(transport.get("headers"), dict) else {}
    host_hdr = str(ws_headers.get("Host") or ws_headers.get("host")
                   or tr_headers.get("Host") or tr_headers.get("host") or "")
    alpn = p.get("alpn")
    alpn_s = ",".join(str(a) for a in alpn) if isinstance(alpn, list) else str(alpn or "")
    aid_raw = p.get("alterId", p.get("alter_id", 0))
    try:
        aid = int(aid_raw)
    except (TypeError, ValueError):
        aid = 0
    return {
        "v": "2", "ps": str(p.get("name") or ""), "add": host, "port": str(port),
        "id": uuid, "aid": str(aid), "scy": str(p.get("cipher") or p.get("security") or "auto"),
        "net": net, "type": "none", "host": host_hdr,
        "path": str(ws_opts.get("path") or transport.get("path") or ""),
        "tls": "tls" if security in ("tls", "reality") else "",
        "sni": sni, "alpn": alpn_s, "fp": fp,
    }


def _vmess_from_proxy(p: dict[str, Any]) -> list[str]:
    """Clash/sing-box vmess-прокси → vmess://base64(JSON)."""
    hp = _addr_port(p)
    uuid = p.get("uuid")
    if isinstance(uuid, (int, float)) and not isinstance(uuid, bool):
        uuid = str(uuid)  # YAML: uuid: 12345678 — число, а не строка
    if hp is None or not isinstance(uuid, str) or not uuid:
        return []
    host, port = hp
    try:
        payload = json.dumps(_vmess_obj(p, host, port, uuid), ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return []
    return ["vmess://" + _std_b64(payload.encode("utf-8"))]


def _hy2_from_proxy(p: dict[str, Any]) -> list[str]:
    """Clash/sing-box hysteria2-прокси → hysteria2:// URL."""
    hp = _addr_port(p)
    if hp is None:
        return []
    host, port = hp
    password = p.get("password") or p.get("auth") or p.get("auth_str") or ""
    if isinstance(password, (int, float)) and not isinstance(password, bool):
        password = str(password)  # YAML: password: 123456 — число, а не строка
    if not isinstance(password, str):
        return []
    q: dict[str, str] = {}
    _security, sni, _fp, _pbk, _sid, insecure = _tls_info(p)
    if sni: q["sni"] = sni
    if insecure: q["insecure"] = "1"
    obfs = p.get("obfs")
    if isinstance(obfs, dict):  # sing-box
        if obfs.get("type"): q["obfs"] = str(obfs["type"])
        if obfs.get("password"): q["obfs-password"] = str(obfs["password"])
    elif isinstance(obfs, str) and obfs:  # Clash
        q["obfs"] = obfs
        op = p.get("obfs-password") or (p.get("obfs-opts") or {}).get("password")
        if op: q["obfs-password"] = str(op)
    query = ("?" + urlencode(sorted(q.items()), safe="")) if q else ""
    userinfo = (quote(password, safe="") + "@") if password else ""
    return [f"hysteria2://{userinfo}{_host_repr(host)}:{port}{query}"]


def _tuic_from_proxy(p: dict[str, Any]) -> list[str]:
    """Clash/sing-box tuic-прокси → tuic:// (v5-формат uuid:password)."""
    hp = _addr_port(p)
    if hp is None:
        return []
    host, port = hp
    uuid = p.get("uuid") or p.get("id") or ""
    password = p.get("password") or ""
    if isinstance(uuid, (int, float)) and not isinstance(uuid, bool):
        uuid = str(uuid)  # YAML: uuid: 12345678 — число, а не строка
    if isinstance(password, (int, float)) and not isinstance(password, bool):
        password = str(password)
    if not isinstance(uuid, str) or not uuid \
            or not (_UUID_RE.match(uuid) or _SECRET_RE.match(uuid)):
        return []
    if not isinstance(password, str):
        return []
    q: dict[str, str] = {}
    cc = str(p.get("congestion_control") or "").strip().lower()
    if cc in ("cubic", "new_reno", "bbr") and cc != "cubic":
        q["congestion_control"] = cc
    urm = str(p.get("udp_relay_mode") or "").strip().lower()
    if urm in ("native", "quic") and urm != "native":
        q["udp_relay_mode"] = urm
    if p.get("udp_over_stream"):
        q["udp_over_stream"] = "1"
    if p.get("zero_rtt_handshake") or p.get("reduce_rtt"):
        q["reduce_rtt"] = "1"
    _security, sni, _fp, _pbk, _sid, insecure = _tls_info(p)
    if sni: q["sni"] = sni
    if insecure: q["allow_insecure"] = "1"
    alpn = p.get("alpn")
    if isinstance(alpn, list) and alpn:
        q["alpn"] = ",".join(str(a) for a in alpn)
    elif isinstance(alpn, str) and alpn:
        q["alpn"] = alpn
    query = ("?" + urlencode(sorted(q.items()), safe="")) if q else ""
    userinfo = f"{quote(uuid, safe='')}:{quote(password, safe='')}"
    return [f"tuic://{userinfo}@{_host_repr(host)}:{port}{query}"]


def _generic_proxy_to_urls(p: dict[str, Any]) -> list[str]:
    """Clash-прокси и sing-box outbound (type + server) → URL — по типу прокси."""
    ptype = str(p.get("type") or "").strip().lower()
    try:
        if ptype == "vless":
            return _vless_from_proxy(p)
        if ptype == "trojan":
            return _trojan_from_proxy(p)
        if ptype == "vmess":
            return _vmess_from_proxy(p)
        if ptype in ("ss", "shadowsocks"):
            return _ss_from_proxy(p)
        if ptype in ("hysteria2", "hy2"):
            return _hy2_from_proxy(p)
        if ptype == "tuic":
            return _tuic_from_proxy(p)
    except Exception:
        return []
    return []


def _stream_query(stream: dict[str, Any]) -> dict[str, str]:
    """streamSettings Xray → query-параметры URL."""
    q: dict[str, str] = {}
    net = str(stream.get("network") or "tcp").lower()
    security = str(stream.get("security") or "none").lower()
    if security == "tls":
        ts = stream.get("tlsSettings") if isinstance(stream.get("tlsSettings"), dict) else {}
        q["security"] = "tls"
        if ts.get("serverName"): q["sni"] = str(ts["serverName"])
        if ts.get("fingerprint"): q["fp"] = str(ts["fingerprint"])
        if isinstance(ts.get("alpn"), list) and ts["alpn"]:
            q["alpn"] = ",".join(str(a) for a in ts["alpn"])
        if ts.get("allowInsecure"): q["allowInsecure"] = "1"
    elif security == "reality":
        rs = stream.get("realitySettings") if isinstance(stream.get("realitySettings"), dict) else {}
        q["security"] = "reality"
        names = rs.get("serverNames") or []
        if isinstance(names, list) and names:
            q["sni"] = str(names[0])
        if rs.get("publicKey"): q["pbk"] = str(rs["publicKey"])
        if rs.get("shortId"): q["sid"] = str(rs["shortId"])
        if rs.get("fingerprint"): q["fp"] = str(rs["fingerprint"])
        if rs.get("spiderX"): q["spx"] = str(rs["spiderX"])
    elif security == "xtls":
        xs = stream.get("xtlsSettings") if isinstance(stream.get("xtlsSettings"), dict) else {}
        q["security"] = "tls"
        if xs.get("serverName"): q["sni"] = str(xs["serverName"])
        if xs.get("fingerprint"): q["fp"] = str(xs["fingerprint"])
    if net not in ("", "tcp"):
        q["type"] = net
        if net == "ws":
            ws = stream.get("wsSettings") if isinstance(stream.get("wsSettings"), dict) else {}
            q["path"] = str(ws.get("path") or "/")
            headers = ws.get("headers") if isinstance(ws.get("headers"), dict) else {}
            h = headers.get("Host") or headers.get("host") or ""
            if h:
                q["host"] = str(h)
        elif net == "grpc":
            g = stream.get("grpcSettings") if isinstance(stream.get("grpcSettings"), dict) else {}
            if g.get("serviceName"):
                q["serviceName"] = str(g["serviceName"])
        elif net == "h2":
            h2 = stream.get("httpSettings") if isinstance(stream.get("httpSettings"), dict) else {}
            q["path"] = str(h2.get("path") or "/")
            hosts = h2.get("host") or []
            if isinstance(hosts, list) and hosts:
                q["host"] = str(hosts[0])
    return q


def _vmess_b64(uid: str, alter_id: Any, security: Any, addr: str, port: int, q: dict[str, str]) -> str:
    try:
        aid = int(alter_id)
    except (TypeError, ValueError):
        aid = 0
    obj = {
        "v": "2", "ps": "", "add": addr, "port": str(port), "id": uid,
        "aid": str(aid), "scy": str(security or "auto"), "net": q.get("type", "tcp"),
        "type": "none", "host": q.get("host", ""), "path": q.get("path", ""),
        "tls": "tls" if q.get("security") in ("tls", "reality") else "",
        "sni": q.get("sni", ""), "alpn": q.get("alpn", ""), "fp": q.get("fp", ""),
    }
    return "vmess://" + _std_b64(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _xray_outbound_to_urls(ob: dict[str, Any]) -> list[str]:
    """Outbound Xray/V2Ray (protocol + settings/streamSettings) → список URL."""
    proto = str(ob.get("protocol") or "").strip().lower()
    conf = ob.get("settings") if isinstance(ob.get("settings"), dict) else {}
    stream = ob.get("streamSettings") if isinstance(ob.get("streamSettings"), dict) else {}
    out: list[str] = []
    try:
        if proto in ("vless", "vmess"):
            q = _stream_query(stream)
            for srv in conf.get("vnext") or []:
                addr_port = _addr_port(srv)
                if addr_port is None:
                    continue
                addr, port = addr_port
                for user in srv.get("users") or []:
                    if not isinstance(user, dict):
                        continue
                    uid = user.get("id")
                    if not isinstance(uid, str) or not uid:
                        continue
                    if proto == "vless":
                        qq = dict(q)
                        qq.setdefault("encryption", str(user.get("encryption") or "none"))
                        if user.get("flow"):
                            qq["flow"] = str(user["flow"])
                        out.append(f"vless://{quote(uid, safe='')}@{_host_repr(addr)}:{port}"
                                   f"?{urlencode(sorted(qq.items()), safe='')}")
                    else:
                        out.append(_vmess_b64(uid, user.get("alterId", 0),
                                              user.get("security", "auto"), addr, port, q))
        elif proto == "trojan":
            q = _stream_query(stream)
            for srv in conf.get("servers") or []:
                addr_port = _addr_port(srv)
                pwd = str(srv.get("password") or "") if isinstance(srv, dict) else ""
                if addr_port is None or not pwd:
                    continue
                addr, port = addr_port
                out.append(f"trojan://{quote(pwd, safe='')}@{_host_repr(addr)}:{port}"
                           + (f"?{urlencode(sorted(q.items()), safe='')}" if q else ""))
        elif proto == "shadowsocks":
            for srv in conf.get("servers") or []:
                addr_port = _addr_port(srv)
                if addr_port is None or not isinstance(srv, dict):
                    continue
                method = str(srv.get("method") or "")
                password = str(srv.get("password") or "")
                if not method:
                    continue
                addr, port = addr_port
                userinfo = _b64_no_pad(f"{method}:{password}".encode("utf-8"))
                out.append(f"ss://{userinfo}@{_host_repr(addr)}:{port}")
    except Exception:
        pass
    return out


def _iter_walk(node: Any, settings: Settings, depth: int) -> Iterator[str]:
    """Рекурсивный обход JSON/YAML-дерева (структура не дублируется в список)."""
    if depth > 12:
        return
    if isinstance(node, list):
        for item in node:
            yield from _iter_walk(item, settings, depth + 1)
        return
    if not isinstance(node, dict):
        return
    if isinstance(node.get("protocol"), str) and ("settings" in node or "streamSettings" in node):
        yield from _xray_outbound_to_urls(node)
        return
    if isinstance(node.get("type"), str) and "server" in node and ("port" in node or "server_port" in node):
        yield from _generic_proxy_to_urls(node)
        return
    for value in node.values():
        yield from _iter_walk(value, settings, depth + 1)


def _iter_configs_from_json(text: str, settings: Settings) -> Iterator[str]:
    try:
        data = json.loads(text)
    except (ValueError, RecursionError):
        return
    yield from _iter_walk(data, settings, 0)


def _iter_configs_from_yaml(text: str, settings: Settings) -> Iterator[str]:
    try:
        data = _yaml_safe_load_limited(text, settings.yaml_node_limit)
    except Exception:  # ошибки парсинга YAML игнорируем
        return
    yield from _iter_walk(data, settings, 0)


# ---------- точка входа парсера (потоковый конвейер) ---------------------------

def iter_extract_from_content(text: str, settings: Settings,
                              stats: dict[str, Any] | None = None) -> Iterator[str]:
    """Потоковый конвейер: URL → Base64 (рекурсивно) → JSON → YAML.

    Не собирает результат в список: потребитель забирает URL по одному,
    поэтому память не растёт пропорционально размеру источника.

    stats (необязательно) — счётчики для отладочного журнала:
      direct    — сколько конфигов найдено прямыми ссылками;
      b64_1..N  — сколько дал каждый уровень декодирования Base64;
      json/yaml — сколько вынуто из JSON/YAML-структур;
      decoded   — суммарно через декодирование (не прямыми ссылками).
    """
    limit = settings.max_configs_per_source
    produced = 0

    def _capped(it: Iterator[str], *, key: str = "", decoded: bool = False) -> Iterator[str]:
        nonlocal produced
        for url in it:
            if produced >= limit:
                return
            produced += 1
            if stats is not None:
                if key:
                    stats[key] = stats.get(key, 0) + 1
                if decoded:
                    stats["decoded"] = stats.get("decoded", 0) + 1
            yield url

    yield from _capped(iter_extract_config_urls(text, settings), key="direct")

    if settings.decode_base64:
        blob = text
        max_chars = max(1, settings.base64_max_len_mb) * _MB
        for level in range(1, max(1, settings.base64_decode_levels) + 1):
            if produced >= limit or not _looks_like_base64(blob, max_chars):
                break
            decoded = _b64_to_text(blob)
            if decoded is None:
                break
            blob = decoded
            yield from _capped(iter_extract_config_urls(blob, settings),
                               key=f"b64_{level}", decoded=True)
            stripped = blob.lstrip().lstrip("\ufeff")
            if settings.decode_json and stripped[:1] in "{[":
                yield from _capped(_iter_configs_from_json(stripped, settings),
                                   key="json", decoded=True)
            if settings.decode_yaml and "proxies" in blob and "type" in blob:
                yield from _capped(_iter_configs_from_yaml(blob, settings),
                                   key="yaml", decoded=True)

    trimmed = text.lstrip().lstrip("\ufeff")
    is_json_like = trimmed[:1] in "{["
    if settings.decode_json and is_json_like \
            and len(text) <= settings.json_yaml_max_len_mb * _MB:
        yield from _capped(_iter_configs_from_json(trimmed, settings), key="json", decoded=True)
    if settings.decode_yaml and not is_json_like \
            and "proxies" in text and "type" in text \
            and len(text) <= settings.json_yaml_max_len_mb * _MB:
        yield from _capped(_iter_configs_from_yaml(text, settings), key="yaml", decoded=True)


def extract_from_content(text: str, settings: Settings) -> list[str]:
    """Списковая обёртка над потоковым конвейером (для небольших данных)."""
    return list(iter_extract_from_content(text, settings))


# ══════════════════════════════════════════════════════════════════════════════
# СЕТЬ: requests + сессия на поток, стриминговое чтение с лимитом размера
# ══════════════════════════════════════════════════════════════════════════════

_net_local = threading.local()  # «поток -> его личная сессия requests»


@dataclass(slots=True)
class FetchResult:
    """Итог HTTP-запроса.

    ok — есть что парсить: и полный ответ, и «обрезанный», из которого
    всё равно можно вынуть конфиги; status — HTTP-код; text — тело
    (насколько дочиталось); size — размер тела в байтах; note — пометка
    об обрыве («ответ прерван после N байт»); error — причина отказа.
    Исключения наружу не выходят никогда: сеть ошибок не жалеет,
    но вызывающему коду ловить их не приходится.
    """
    ok: bool
    status: int = 0
    text: str = ""
    error: str = ""
    size: int = 0
    note: str = ""


def _get_session() -> requests.Session:
    """Сессия requests, закреплённая за текущим потоком.

    У каждого воркера своя: keep-alive соединения переживают одиночный
    запрос, а пул сокетов не делят конкурирующие потоки. SSL здесь не
    настраиваем — у каждого запроса свой verify (см. fetch_url).
    """
    session = getattr(_net_local, "session", None)
    if session is None:
        session = requests.Session()
        _net_local.session = session
    return session


def fetch_url(
    url: str,
    *,
    user_agent: str,
    timeout_connect: int,
    timeout_read: int,
    max_mb: float,
    method: str = "GET",
    extra_params: dict[str, str] | None = None,
    extra_headers: dict[str, str] | None = None,
    verify: bool = False,
    on_progress: "Any | None" = None,
    max_seconds: float = 0,
) -> FetchResult:
    """Скачивает ответ по HTTP и возвращает готовый текст.

    extra_headers добавляются к базовым User-Agent/Accept — так Happ-заход
    шлёт заголовки случайного устройства (X-HWID, X-Device-*).

    Тело читается потоком, кусками по 64 КБ, и копится в памяти лишь до
    лимита max_mb: «бесконечный» ответ обрезается и считается ошибкой,
    а не съедает память.

    on_progress(прочитано_байт, всего_байт) вызывается на каждом куске.
    Всего_байт берётся из заголовка Content-Length, а если сервер его не
    прислал — равен нулю. Воркеры через этот колбэк отмечаются в таблице
    живости (дедлайн-контроль видит, что источник отдаёт данные), а меню
    скачивания рисует по этим числам прогресс-бар.

    max_seconds — общий предел времени на весь ответ (0 — без предела):
    «вечный» сервер, поливающий по байту в минуту, отпускается по его
    истечении, и всё накопленное разбирается как обычный обрыв — конфиги
    из него достаются, а поток не занят вечно.

    Обрыв по таймауту чтения — не всегда приговор: если байты уже шли,
    возвращаем то, что успело прийти (ok=True, note с пометкой). Конфиги
    из «обрезанного» ответа ценны ровно так же, как из целого, а source,
    зависший посреди передачи, больше не теряет всё накопленное. Все
    прочие неприятности — DNS, SSL, петли перенаправлений — складываются
    в FetchResult с ok=False, ловить исключения не нужно.
    """
    session = _get_session()
    headers = {"User-Agent": user_agent, "Accept": "*/*"}
    if extra_headers:
        headers.update(extra_headers)  # заголовки устройства для Happ-захода
    try:
        with session.request(
            method, url, params=extra_params, headers=headers,
            timeout=(max(1, timeout_connect), max(1, timeout_read)),
            stream=True, verify=verify, allow_redirects=True,
        ) as resp:
            if resp.status_code >= 400:
                return FetchResult(False, resp.status_code, "", f"HTTP {resp.status_code}")
            limit = int(max_mb * 1024 * 1024)
            # Тело копится в ОДНОМ растущем буфере: потолок памяти — два
            # экземпляра ответа (буфер + декодированная строка), тогда как
            # список кусков + join + строка давали три. На больших подписках
            # это заметно снижает расход памяти.
            buf = bytearray()
            total = 0
            note = ""
            started_at = time.monotonic()
            try:
                total_len = int(resp.headers.get("Content-Length") or 0)
            except (TypeError, ValueError):
                total_len = 0
            try:
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > limit:
                        return FetchResult(False, resp.status_code, "", "превышен лимит размера ответа")
                    buf += chunk
                    if on_progress is not None:
                        try:
                            on_progress(total, total_len)
                        except Exception:
                            pass  # колбэк не должен ронять загрузку
                    if max_seconds > 0 and time.monotonic() - started_at > max_seconds:
                        note = (f"остановлен по лимиту времени после {total} байт "
                                f"({total // 1024} КБ)")
                        break
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
                if total == 0:
                    raise  # ни байта не пришло — это честный провал
                # ответ прерван посреди передачи — работаем с тем, что есть
                note = f"ответ прерван после {total} байт ({total // 1024} КБ)"
            encoding = resp.encoding or "utf-8"
            try:
                # str() от bytearray декодирует прямо из буфера, без
                # промежуточной копии bytes — минус один экземпляр данных
                text = str(buf, encoding, errors="replace")
            except LookupError:
                text = str(buf, "utf-8", errors="replace")
            size = total
            buf.clear()  # буфер больше не нужен — строка уже собрана
            del buf
            return FetchResult(True, resp.status_code, text, "", size, note)
    except requests.exceptions.SSLError:
        return FetchResult(False, error="ошибка SSL")
    except requests.exceptions.ConnectTimeout:
        return FetchResult(False, error="таймаут соединения")
    except requests.exceptions.ReadTimeout:
        return FetchResult(False, error="таймаут чтения")
    except requests.exceptions.TooManyRedirects:
        return FetchResult(False, error="слишком много перенаправлений")
    except requests.exceptions.RequestException as exc:
        return FetchResult(False, error=type(exc).__name__)
    except Exception as exc:  # совсем неожиданное — поток обязан выжить
        return FetchResult(False, error=str(exc)[:100])


def _retryable(res: FetchResult) -> bool:
    """Имеет ли смысл переспросить после такого ответа.

    403/404 и прочие честные «нет» переспрашивать бессмысленно — сервер
    уже ответил. А вот таймауты, обрывы и серверные 5xx часто лечатся
    второй попыткой, потому повторяем только их.
    """
    if res.ok:
        return False
    if res.status == 0:  # до HTTP не дошло: DNS, таймаут, обрыв
        return True
    return res.status in (408, 425, 429, 500, 502, 503, 504)


# ══════════════════════════════════════════════════════════════════════════════
# GITHUB-ФИКС: SHA-хэш → main/master
# ══════════════════════════════════════════════════════════════════════════════

# Raw-ссылка с хэшем коммита (…/user/repo/<sha>/файл) умирает, как только
# репозиторий переписывают силой: коммита больше нет, сервер отвечает 404.
# Лечение — подставить вместо хэша имя живой ветки: почти всегда это main,
# в старых репозиториях master. Живость ветки подтверждаем HEAD-запросом,
# чтобы не записать в sources.txt новую мёртвую ссылку.
_GH_HASH_RE = re.compile(
    r"^(?P<prefix>https://raw\.githubusercontent\.com/[^/\s]+/[^/\s]+/"
    r"|https://github\.com/[^/\s]+/[^/\s]+/raw/)"
    r"(?P<sha>[0-9a-fA-F]{7,64})"
    r"(?P<rest>/\S*)$"
)


def gh_needs_fix(url: str) -> bool:
    """Raw-ссылка содержит хэш коммита вместо имени ветки."""
    return _GH_HASH_RE.match(url) is not None


def _gh_fix_one(url: str, settings: Settings) -> str | None:
    """Подбирает живую ветку вместо хэша коммита.

    Кандидаты — main, затем master; каждому отправляется HEAD-запрос с
    крошечным лимитом тела (содержимое не нужно, важен только код).
    Код меньше 400 означает, что ветка существует и ссылка рабочая.
    Ни одна не подтвердилась — None, чинить нечего.
    """
    match = _GH_HASH_RE.match(url)
    if match is None:
        return None
    prefix, rest = match.group("prefix"), match.group("rest")
    for branch in ("main", "master"):
        candidate = f"{prefix}{branch}{rest}"
        result = fetch_url(
            candidate, user_agent=settings.user_agent,
            timeout_connect=settings.timeout_normal, timeout_read=settings.timeout_normal,
            max_mb=0.001, method="HEAD", verify=settings.verify_ssl,
        )
        if result.ok:
            return candidate
    return None


def gh_fix_source_lines(lines: list[str], settings: Settings, console: Console,
                        pool: "DaemonPool") -> tuple[list[str], int, int]:
    """Проверяет все raw-ссылки с хэшами и чинит их на месте.

    Ссылки проверяются параллельно в daemon-пуле; HEAD-запрос, зависший
    дольше дедлайна, бросается и считается непроверенным. Если
    исправленная ссылка повторяет уже записанную в файл — дубль не
    пишем. Возвращает (строки файла после правки, число замен, число
    отброшенных дублей).
    """
    unique: list[str] = []
    seen: set[str] = set()
    for line in lines:
        url = clean_source_line(line)
        if is_url_line(url) and url not in seen:
            seen.add(url)
            unique.append(url)
    targets = [u for u in unique if gh_needs_fix(u)]
    console.info("Замена Raw-ссылок с хэшем на main/master (с проверкой)")
    if not targets:
        return lines, 0, 0

    # HEAD-проверки лёгкие — им хватает короткого дедлайна, но не меньше 15 сек
    deadline = max(15, settings.source_deadline_seconds or 60)
    pool.set_max(settings.github_fix_threads)
    pool.bump_gen()  # непроверенные задачи прошлого захода выбрасываются
    total = len(targets)
    fixed: dict[str, str] = {}
    pending: dict[Future, tuple[str, float]] = {}
    for url in targets:
        pending[pool.submit(_gh_fix_one, url, settings)] = (url, time.monotonic())

    checked = 0
    try:
        while pending:
            # ждём первые завершившиеся проверки недолго: между ожиданиями
            # надо успевать прокручивать дедлайн и прогресс
            done_set, _ = wait(set(pending), timeout=0.2, return_when=FIRST_COMPLETED)
            for fut in done_set:
                url, _t0 = pending.pop(fut)
                checked += 1
                try:
                    new_url = fut.result()
                except Exception:
                    new_url = None
                if new_url:
                    fixed[url] = new_url
            now = time.monotonic()
            for fut, (url, t0) in list(pending.items()):
                if now - t0 >= deadline:
                    # проверка зависла — её результат уже никому не нужен
                    pending.pop(fut, None)
                    fut.cancel()
                    checked += 1
            pct = min(100, checked * 100 // total)
            console.progress(f"{console.bar(checked, total)} "
                             f"{console.paint(str(pct) + '%', console.accent, bold=True)} | "
                             f"Ссылок: {checked}/{total}")
    finally:
        for fut in pending:
            fut.cancel()
        pool.bump_gen()  # не начатые проверки выбрасываем
        console.clear_progress()

    if not fixed:
        return lines, 0, 0

    known = set(seen)
    out: list[str] = []
    replaced = dropped = 0
    for line in lines:
        url = clean_source_line(line)
        if is_url_line(url) and url in fixed:
            new_url = fixed[url]
            if new_url in known:
                dropped += 1  # починенная ссылка повторяет уже записанную
                continue
            known.add(new_url)
            out.append(new_url)
            replaced += 1
        else:
            out.append(line)
    return out, replaced, dropped


# ══════════════════════════════════════════════════════════════════════════════
# ХРАНИЛИЩЕ: дедупликация по 16-байтовому BLAKE2b, запись в реальном времени
# ══════════════════════════════════════════════════════════════════════════════

class ConfigStore:
    """Дедупликатор и кладовая configs.txt.

    Дедупликационные ключи лежат не в Python-множестве (там каждый
    объект bytes обходится в ~80 байт с накладными расходами), а в
    самодельной открытой хеш-таблице поверх bytearray: ровно 8 байт на
    запись. Миллион конфигов — около 8–16 МБ вместо ~80, миллионы —
    десятки, а не сотни мегабайт: именно это держит общее потребление
    ОЗУ в разумных пределах даже на огромном configs.txt. Ключ — первые
    8 байт BLAKE2b-дайджеста строки: вероятность коллизии на миллионах
    записей порядка 10^-6. Таблица растёт вдвое при заполнении 70%
    и переживает все циклы.
    """

    _ENTRY: int = 8           # байт на запись (усечённый дайджест)
    _START_BITS: int = 16     # старт: 65536 слотов = 0.5 МБ

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._bits: int = self._START_BITS
        self._table = bytearray((1 << self._bits) * self._ENTRY)
        self._count = 0

    @staticmethod
    def _hash(line: str) -> bytes:
        d = hashlib.blake2b(line.encode("utf-8"), digest_size=16).digest()[:8]
        return d if any(d) else b"\x01"  # сплошные нули — признак пустого слота

    @property
    def total(self) -> int:
        with self._lock:
            return self._count

    # ----- низкоуровневая таблица: все операции — под self._lock -----
    def _probe(self, digest: bytes) -> bool:
        """True — дайджест уже в таблице (линейное пробирование)."""
        entry = self._ENTRY
        table = self._table
        mask = (1 << self._bits) - 1
        pos = int.from_bytes(digest, "little") & mask
        while True:
            slot = pos * entry
            chunk = table[slot:slot + entry]
            if chunk == digest:
                return True
            if not any(chunk):
                return False
            pos = (pos + 1) & mask

    def _insert(self, digest: bytes) -> None:
        """Вставляет дайджест (место заведомо свободно); при надобности растёт."""
        if (self._count + 1) * 10 >= (1 << self._bits) * 7:
            self._grow_locked()
        entry = self._ENTRY
        table = self._table
        mask = (1 << self._bits) - 1
        pos = int.from_bytes(digest, "little") & mask
        while any(table[pos * entry:pos * entry + entry]):
            pos = (pos + 1) & mask
        table[pos * entry:pos * entry + entry] = digest
        self._count += 1

    def _grow_locked(self) -> None:
        """Удваивает таблицу и раскладывает ключи заново."""
        old = self._table
        old_slots = 1 << self._bits
        self._bits += 1
        entry = self._ENTRY
        new_table = bytearray((1 << self._bits) * entry)
        mask = (1 << self._bits) - 1
        empty = b"\x00" * entry
        for i in range(old_slots):
            chunk = bytes(old[i * entry:(i + 1) * entry])
            if chunk == empty:
                continue
            pos = int.from_bytes(chunk, "little") & mask
            while new_table[pos * entry:pos * entry + entry] != empty:
                pos = (pos + 1) & mask
            new_table[pos * entry:pos * entry + entry] = chunk
        self._table = new_table
        del old

    def load_existing(self) -> int:
        """Построчная загрузка дедупликационных ключей (файл НЕ читается целиком в память)."""
        count = 0
        try:
            with open(self.path, "r", encoding="utf-8-sig", errors="replace") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        digest = self._hash(stripped)
                        if not self._probe(digest):
                            self._insert(digest)
                        count += 1
        except OSError:
            pass
        return count

    def add(self, urls: Iterable[str]) -> int:
        """Дописывает в конец файла конфиги, которых там ещё нет.

        Входная пачка чистится от повторов — и с прошлыми циклами, и
        внутри себя. Строки пишутся по одной, с flush после каждой: при
        обрыве в середине учтёнными останутся ровно те, что успели
        попасть на диск, и файл не разойдётся с таблицей (в таблицу
        ключ попадает только после успешной записи). Возвращает число
        дописанных строк.
        """
        added = 0
        with self._lock:
            fresh: list[str] = []
            digests: list[bytes] = []
            batch_seen: set[bytes] = set()
            for url in urls:
                digest = self._hash(url)
                if digest in batch_seen or self._probe(digest):
                    batch_seen.add(digest)
                    continue
                batch_seen.add(digest)
                fresh.append(url)
                digests.append(digest)
            if fresh:
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    with open(self.path, "a", encoding="utf-8") as f:
                        for url, digest in zip(fresh, digests):
                            f.write(url + "\n")
                            f.flush()
                            self._insert(digest)  # ключ — только за реально записанную строку
                            added += 1
                except OSError:
                    pass  # записанное учтено; остальное попробуем в следующий раз
        return added


# ══════════════════════════════════════════════════════════════════════════════
# ПУЛ DAEMON-ПОТОКОВ: зависший воркер не блокирует ни цикл, ни выход из программы
# ══════════════════════════════════════════════════════════════════════════════

class DaemonPool:
    """Пул рабочих потоков с «поколениями» задач — для сетевых запросов.

    Почему не ThreadPoolExecutor из стандартной библиотеки:
      * его потоки — не демоны. Воркер, застрявший в DNS-резолве или в
        «капельном» ответе сервера, держит процесс открытым даже после
        Ctrl+C — прервать такое ожидание штатным способом нельзя. Здесь
        потоки daemon, и программа закрывается, не дожидаясь застрявших;
      * зависшую задачу нельзя заставить завершиться — её можно только
        бросить. Поколения (gen) и есть способ «забыть»: новому циклу
        соответствует новое поколение, и ещё не начатые задачи старого
        отбрасываются без исполнения.

    Потоки не умирают между задачами и живут столько же, сколько всё
    приложение, поэтому thread-local сессии requests и их keep-alive
    соединения переиспользуются от цикла к циклу.
    """

    def __init__(self, max_workers: int, name: str = "worker") -> None:
        self._max = 1
        self._name = name
        self._queue: queue.SimpleQueue = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._alive = 0   # воркер-потоков запущено и живо
        self._busy = 0    # из них прямо сейчас исполняют задачу (зависшие — вечно)
        self.gen = 0
        self.set_max(max_workers)

    def set_max(self, max_workers: int) -> None:
        """Задаёт число рабочих потоков.

        Рост — немедленный (новые потоки стартуют сразу). Усадка —
        мягкая: лишние воркеры уходят сами, как только освободятся от
        текущей задачи, а простаивающие — при ближайшей проверке очереди
        (см. _worker), не роняя чужих результатов.
        """
        with self._lock:
            self._max = max(1, max_workers)
            self._ensure_threads_locked()

    def _ensure_threads_locked(self) -> None:
        while self._alive < self._max:
            self._alive += 1
            try:
                threading.Thread(target=self._worker, daemon=True,
                                 name=f"{self._name}-{self._alive}").start()
            except Exception:
                # поток не стартовал (лимит ОС) — слота за счётчиком нет
                self._alive -= 1
                break

    def ensure_capacity(self) -> None:
        """Восполняет ёмкость пула, съеденную зависшими воркерами.

        Воркер, навсегда застрявший в сетевом запросе (например, в
        DNS-резолве без таймаута), к очереди уже не вернётся — а слот за
        ним держится, и пул от цикла к циклу монотонно теряет ёмкость.
        В начале цикла досчитываем свободных (не занятых задачей) воркеров
        до максимума: недостающие стартуют на ту же очередь тем же
        воркером. Потолок — удвоенный максимум: зависшие потоки остаются
        в счёте живых, и без потолка пул разбухал бы бесконечно.
        """
        with self._lock:
            free = self._alive - self._busy
            while free < self._max and self._alive < self._max * 2:
                self._alive += 1
                try:
                    threading.Thread(target=self._worker, daemon=True,
                                     name=f"{self._name}-{self._alive}").start()
                except Exception:
                    self._alive -= 1
                    break
                free += 1

    def bump_gen(self) -> int:
        """Открывает новое поколение задач.

        Всё, что ещё стоит в очереди от старого, воркеры выбросят не
        глядя. Вызывается в начале цикла и при его сворачивании.
        """
        self.gen += 1
        return self.gen

    def submit(self, fn: Any, *args: Any) -> Future:
        """Ставит задачу в очередь текущего поколения; при нужде добавляет потоков."""
        fut: Future = Future()
        with self._lock:
            self._queue.put((self.gen, fut, fn, args))
            self._ensure_threads_locked()
        return fut

    def _worker(self) -> None:
        # gone — счётчик _alive уже уменьшен в теле (выход по усадке из
        # простоя); финальный блок тогда второй раз не уменьшает.
        gone = False
        try:
            while True:
                try:
                    gen, fut, fn, args = self._queue.get(timeout=0.5)
                except queue.Empty:
                    # Очередь пуста: если воркеров больше, чем нужно
                    # (усадка пула), лишний простаивающий воркер уходит сам.
                    # Уменьшение счётчика и решение — под одним замком,
                    # чтобы два воркера не ушли «одним слотом».
                    with self._lock:
                        if self._alive <= self._max:
                            continue
                        self._alive -= 1
                        gone = True
                    return
                if gen != self.gen or fut.cancelled():
                    # задача из прошлого поколения или уже отменена — выбрасываем
                    fut.cancel()
                    continue
                if fut.set_running_or_notify_cancel():
                    with self._lock:
                        self._busy += 1
                    try:
                        fut.set_result(fn(*args))
                    except BaseException as exc:  # исключение не должно убивать поток
                        fut.set_exception(exc)
                    finally:
                        with self._lock:
                            self._busy -= 1
                            over = self._alive > self._max
                    # при усадке пула воркер уходит, счётчик _alive поправит внешний finally
                    if over:
                        return
        finally:
            # Воркеры сами не умирают, но если поток всё же кончился, пул
            # обязан об этом узнать — иначе слот числится занятым вечно
            # и восполнять будет нечего (см. ensure_capacity).
            if not gone:
                with self._lock:
                    self._alive -= 1

# ══════════════════════════════════════════════════════════════════════════════
# ЦИКЛ ЗАГРУЗКИ: daemon-пул, скользящее окно, дедлайн, Happ-фолбэк, Ctrl+C
# ══════════════════════════════════════════════════════════════════════════════

# Схема ссылки -> переключатель в настройках, разрешающий её сохранение.
# Схем в таблице нет — значит, настройками не фильтруется (как http).
_SCHEME_TO_TOGGLE: dict[str, str] = {
    "vless": "save_vless", "trojan": "save_trojan",
    "ss": "save_ss", "shadowsocks": "save_ss",
    "vmess": "save_vmess",
    "hysteria2": "save_hy2", "hy2": "save_hy2",
    "tuic": "save_tuic",
}

_ADD_BATCH: int = 500        # сколько найденных конфигов копится до записи в configs.txt
_REWRITE_DEBOUNCE: float = 5.0  # пауза между перезаписями sources.txt — не нагружать диск зря

# Пометки о решении по чёрному списку — для подробного режима в консоли.
_BL_ACTION_LABELS: dict[str, str] = {
    "blacklisted": "→ в чёрный список",
    "cooldown": "→ в ЧС отложено (кулдаун)",
    "rehabilitated": "→ убран из чёрного списка",
    "disabled": "",
}


@dataclass(slots=True)
class SourceResult:
    """Итог обработки одного источника — заполняет воркер.

    found/added — сколько конфигов найдено и сколько из них записано
    новыми; protocols — найденное в разрезе схем (vless, vmess, ...);
    decoded — сколько вынуто из Base64/JSON/YAML-контейнеров; via_happ —
    сколько дал резервный заход через Happ; bl_action — что решил чёрный
    список (отправлен / отложен по кулдауну / реабилитирован);
    cancelled — источник снят по Ctrl+C, а не умер по своей вине.
    """
    url: str
    ok: bool
    found: int = 0
    added: int = 0
    error: str = ""
    cancelled: bool = False
    protocols: dict[str, int] = field(default_factory=dict)
    decoded: int = 0
    via_happ: int = 0
    bl_action: str = ""


@dataclass
class _Counters:
    """Сводная статистика цикла.

    Обновляет только главный поток, поэтому замки не нужны.
    """
    done: int = 0
    ok: int = 0
    fail: int = 0
    added: int = 0
    found: int = 0            # найдено конфигов всего
    decoded: int = 0          # найдено внутри Base64/JSON/YAML
    happ: int = 0             # найдено через резервный Happ-заход
    deadlines: int = 0        # источников, которых перестали ждать по дедлайну
    deadlines_kept: int = 0   # ...из них отдавших конфиги — зачтены как успешные
    deadlines_unstarted: int = 0  # ...из них так и не начавших работу из очереди
    cooldown_skipped: int = 0 # в ЧС не отправлены из-за кулдауна
    rehabilitated: int = 0    # вычеркнуты из ЧС, потому что заработали
    protocols: dict[str, int] = field(default_factory=dict)


class Downloader:
    """Скачивание по всем источникам: цикл, пул потоков, чёрный список, статистика."""

    def __init__(self, settings: Settings, console: Console, debug: DebugLog) -> None:
        self.s = settings
        self.c = console
        self.debug = debug
        self.store = ConfigStore(CONFIGS_FILE)
        self.bl_book = BlacklistBook(COOLDOWN_FILE)
        # ЧС живёт в sources/blacklist_sources.txt — ровно там, где его
        # читает Telegram_Checker. Если от прошлой версии осталась копия
        # в temp/, сливаем её в основной файл, пока чтения ещё не начались.
        self._migrate_blacklist_to_sources()
        # просьба пользователя остановиться: воркеры сверяются с ней
        # до сети и после
        self.stop_event = threading.Event()
        self._bl_lock = threading.Lock()
        self._bl_seen: set[str] = set()
        # Таблица «источник -> что он уже успел»: [конфигов найдено,
        # записано новых, байт принято, занят ли разбором, когда взят].
        # Воркеры пишут в неё по ходу работы, дедлайн-контроль читает:
        # если воркер завис, но конфиги уже капают (или капают байты
        # ответа) — источник живой, и в ЧС ему нечего делать, а его
        # конфиги тем временем доучитываются в общий зачёт.
        self._live_lock = threading.Lock()
        self._live: dict[str, list[int]] = {}
        # Дедлайн-источники, чьи воркеры ещё дорабатывают в фоне:
        # Future -> [url, найдено учтено, новых учтено]. Дельты уходят в
        # счётчики по мере поступления — статистика и configs.txt идут нога в ногу.
        self._settling: dict[Future, list] = {}
        # Реабилитированные за цикл: из файла ЧС вычеркнем их одной записью.
        self._rehab_lock = threading.Lock()
        self._rehab_pending: set[str] = set()
        # Кто за цикл реально отправлен в ЧС — их вычёркиваем из sources.txt.
        self._removed_lock = threading.Lock()
        self._removed: set[str] = set()
        # Пути, по которым запись в файл уже ругалась в этой сессии:
        # повторять одно и то же сообщение на каждый чих не нужно.
        self._warned_writes: set[str] = set()
        # Конвейер (run_watch): источники, которые уже видели в sources.txt.
        # Новым считается URL, которого в наборе нет; набор наполняется
        # при каждом чтении файла и не даёт своим же перезаписям файла
        # запустить лишний мини-цикл.
        self._known_sources: set[str] = set()
        # пулы создаются один раз и переживают все циклы — потоки и их
        # сессии переиспользуются, пересоздавать их на каждый цикл дорого
        self.http_pool = DaemonPool(settings.http_threads, "dl")
        self.gh_pool = DaemonPool(settings.github_fix_threads, "ghfix")
        # Устройство для Happ-захода берётся из настроек при каждом заходе
        # (_fetch_happ): сгенерировали в меню — следующий цикл уже с ним.

    # ------------------------------------------------------------------ служебное
    def _sync_debug_logger(self) -> None:
        """Журнал включается и выключается вслед за настройкой debug_logs."""
        if self.s.debug_logs and not self.debug.enabled:
            self.debug.enable()
        elif not self.s.debug_logs and self.debug.enabled:
            self.debug.disable()

    def _live_note(self, url: str, *, configs: int = 0, added: int = 0) -> None:
        """Воркер отмечает: «я живой, я уже отдал конфиги».

        Вызывается на каждом найденном конфиге и на каждой записанной
        пачке. Дедлайн-контроль глядит в эту таблицу, прежде чем считать
        источник мёртвым.
        """
        with self._live_lock:
            entry = self._live.get(url)
            if entry is None:
                self._live[url] = entry = [0, 0, 0, 0, 0.0]
            entry[0] += configs
            entry[1] += added

    def _live_start(self, url: str) -> None:
        """Воркер взялся за источник: он уже «в работе», а не в очереди."""
        with self._live_lock:
            entry = self._live.get(url)
            if entry is None:
                self._live[url] = [0, 0, 0, 1, time.monotonic()]
            else:
                entry[3] = 1
                entry[4] = time.monotonic()

    def _live_size(self, url: str, size: int) -> None:
        """Ответ капает: воркер сообщает, сколько байт уже пришло."""
        with self._live_lock:
            entry = self._live.get(url)
            if entry is None:
                self._live[url] = entry = [0, 0, size, 1, 0.0]
            else:
                entry[2] = max(entry[2], size)

    def _live_parse_done(self, url: str) -> None:
        """Разбор ответа закончен: дальше источник ничем не занят по делу.

        После этой отметки дедлайн больше не спасает источник «размером
        ответа»: содержимое распарсено, конфиги посчитаны (или их нет).
        """
        with self._live_lock:
            entry = self._live.get(url)
            if entry is not None:
                entry[3] = 0

    def _live_started(self, url: str) -> float | None:
        """Когда воркер взялся за источник; None — всё ещё стоит в очереди."""
        with self._live_lock:
            entry = self._live.get(url)
            return (entry[4] or None) if entry else None

    def _live_get(self, url: str) -> tuple[int, int, int, int]:
        with self._live_lock:
            entry = self._live.get(url)
            return (entry[0], entry[1], entry[2], entry[3]) if entry else (0, 0, 0, 0)

    # ------------------------------------------------------------------ публичное
    def run_cycle(self) -> bool:
        """Один полный проход по sources.txt.

        Шаги по порядку:
          1. Проверка sources.txt — если файла нет (или в нём нет ссылок),
             список источников скачивается из интернета.
          2. GitHub-фикс — перепроверяет raw-ссылки с хэшами, подменяет
             хэши на живые ветки и сразу фиксирует правки в sources.txt.
          3. Самопроверка — вычёркивает ссылки, уже числящиеся в чёрном
             списке (в режиме «Кулдаун» — с учётом срока давности).
          4. Скачивание — пул потоков со скользящим окном: конфиги
             дописываются в configs.txt пачками и сразу попадают в
             счётчики (включая фоновый добор от дедлайн-источников),
             провалившиеся источники уезжают в чёрный список по выбранному
             методу, а зависшие перестают ждать по дедлайну — но их
             воркеры дорабатывают в фоне, и конфиги не теряются.

        Возвращает True, если пользователь остановил цикл (Ctrl+C);
        накопленное к этому моменту уже сохранено.
        """
        s, c = self.s, self.c
        interrupted = False
        self.stop_event.clear()
        self._sync_debug_logger()
        if s.clear_screen:
            c.clear_screen()
        c.header("Скачивание конфигураций из подписок")
        # Зависшие в прошлых циклах воркеры ёмкость не вернут — добираем
        # свободных до максимума, пока пулы ещё не в деле.
        self.http_pool.ensure_capacity()
        self.gh_pool.ensure_capacity()

        if self.debug.enabled:
            self.debug.log("Цикл", "старт")
            for key, value in s.to_dict().items():
                self.debug.raw(f"  {key} = {value!r}")

        self._ensure_sources_file()

        existing = self.store.load_existing()
        raw_lines = read_lines_safe(SOURCES_FILE)

        # GitHub-фикс: хэши коммитов заменяем на живые ветки и сразу
        # фиксируем результат в sources.txt. Файл перечитывается под
        # замком: ссылки, дописанные соседями во время фикса (он ходит
        # в сеть и может занять минуты), сохраняются.
        if s.fix_github_urls:
            snapshot_urls = {clean_source_line(ln) for ln in raw_lines
                             if is_url_line(clean_source_line(ln))}
            raw_lines, replaced, dropped = gh_fix_source_lines(raw_lines, s, c, self.gh_pool)
            if replaced or dropped:
                self._update_sources_file(
                    lambda line, url: url is None or url not in snapshot_urls,
                    extra_lines=raw_lines, context="github-фикс")
            c.info(f"Исправлено ссылок: {replaced}"
                   + (f" (дублей удалено: {dropped})" if dropped else ""))

        # строка файла — либо ссылка-источник, либо произвольный текст
        # (комментарий, заметка): текст не трогаем, ссылки пойдут в работу
        entries: list[tuple[str, str | None]] = []
        for line in raw_lines:
            cleaned = clean_source_line(line)
            url = cleaned if is_url_line(cleaned) else None
            entries.append((line, url))

        # Реабилитированных в прошлый цикл вычёркиваем из файла ЧС до того,
        # как он будет прочитан — иначе их снова сочтут мёртвыми.
        self._flush_rehabilitations(force=True)
        # Если прошлый запуск оборвался до перезаписи sources.txt, в файле
        # могли остаться ссылки, уже отправленные в чёрный список.
        # Убираем их сейчас — скачивать их всё равно не стали бы. В режиме
        # «Кулдаун» недавним отправкам даётся отсрочка: попали в ЧС недавно,
        # значит получат ещё одну попытку — вдруг источник ожил.
        self._bl_seen = {ln.strip() for ln in iter_file_lines(BLACKLIST_FILE) if ln.strip()}
        if s.save_failed_sources and self._bl_seen:
            blocked = [url for _, url in entries if url is not None and url in self._bl_seen]
            drop: set[str] = set()
            cooldown_kept = 0
            for url in blocked:
                if s.blacklist_mode == "cooldown" \
                        and self.bl_book.on_cooldown(url, s.blacklist_cooldown_hours):
                    cooldown_kept += 1
                    self.debug.log("ЧС", f"{url} — оставлен в sources.txt: "
                                         f"кулдаун {s.blacklist_cooldown_hours} ч не истёк")
                else:
                    drop.add(url)
            if drop:
                entries = [(line, url) for line, url in entries
                           if url is None or url not in drop]
                self._update_sources_file(
                    lambda line, url: url is None or url not in drop,
                    context="самопроверка ЧС")
                c.info(f"Самопроверка: из sources.txt удалено ранее заблокированных "
                       f"источников: {len(drop)}")
            if cooldown_kept:
                c.debug(f"Самопроверка: источников из ЧС оставлено в работе "
                        f"(кулдаун не истёк): {cooldown_kept}")

            # Режим «Кулдаун»: кто недавно уехал в ЧС — ещё получает шансы.
            # Возвращаем таких в sources.txt, если их там нет: вдруг за
            # сутки источник очнулся. Кулдаун истёк — значит, шансов было
            # достаточно, и ссылка остаётся вычеркнутой.
            if s.blacklist_mode == "cooldown":
                current_urls = {url for _, url in entries if url is not None}
                revived = [url for url in self._bl_seen
                           if url not in current_urls
                           and self.bl_book.on_cooldown(url, s.blacklist_cooldown_hours)]
                if revived:
                    entries.extend((url, url) for url in revived)
                    self._update_sources_file(
                        lambda line, url: True,
                        extra_lines=revived, context="возврат из кулдауна")
                    c.debug(f"Кулдаун ЧС: в sources.txt возвращено источников: {len(revived)}")
                    for url in revived:
                        self.debug.log("ЧС", f"{url} — возвращён в sources.txt: "
                                             f"кулдаун ещё не истёк, даём повторную попытку")

        # одна и та же ссылка может встретиться в файле дважды —
        # каждый URL обрабатываем ровно один раз
        proc_urls: list[str] = []
        seen_proc: set[str] = set()
        for _, url in entries:
            if url and url not in seen_proc:
                seen_proc.add(url)
                proc_urls.append(url)
        del seen_proc

        total = len(proc_urls)
        # Запоминаем, кого цикл уже видел: ссылки, обработанные этим
        # циклом, не считаются «новыми» для добора и подхвата (свой
        # github-фикс или возврат из кулдауна не должны запускать мини-цикл).
        self._known_sources |= set(proc_urls)
        counters = _Counters()
        # состояние перезаписи sources.txt: сколько мёртвых ссылок записано
        # и когда была последняя запись (см. _persist_failures)
        rw_state: dict[str, Any] = {"n": 0, "ts": 0.0}
        with self._removed_lock:
            self._removed.clear()
        with self._live_lock:
            self._live.clear()
        self._settling.clear()

        c.info(f"Обработка {total} источников")
        c.info(f"Конфигураций в файле: {existing}")

        if total == 0:
            c.warn("Нет источников для загрузки (sources.txt пуст или не содержит URL).")
            self._print_summary(counters, cancelled=0, total=0)
            return False

        # Скользящее окно: в полёте не больше `window` задач; слот
        # освободился — отправляем следующую. Память не растёт вместе с
        # sources.txt, а задача, зависшая сверх дедлайна, бросается.
        deadline = max(0, s.source_deadline_seconds)
        window = max(4, s.http_threads * 2)
        self.http_pool.set_max(s.http_threads)
        self.http_pool.bump_gen()
        urls_iter = iter(proc_urls)
        pending: dict[Future, tuple[str, float]] = {}
        abandoned: set[Future] = set()

        def _submit(url: str) -> None:
            pending[self.http_pool.submit(self._process_source, url)] = (url, time.monotonic())

        try:
            # стартовое заполнение окна — дальше только дополняем
            for _ in range(min(window, total)):
                _submit(next(urls_iter))

            last_done, last_ts = -1, time.monotonic()
            while pending:
                done_set, _ = wait(set(pending), timeout=0.2, return_when=FIRST_COMPLETED)
                for fut in done_set:
                    pending.pop(fut, None)
                    if fut not in abandoned:
                        self._collect(fut, counters)
                # отправленные в ЧС ссылки вычёркиваем из sources.txt по ходу цикла
                self._persist_failures(rw_state)
                # освободившиеся слоты сразу заполняем следующими источниками
                while not self.stop_event.is_set() and len(pending) < window:
                    try:
                        _submit(next(urls_iter))
                    except StopIteration:
                        break

                # Дедлайн: источник, не уложившийся в срок, перестаём ждать —
                # цикл не будет ждать один зависший сервер бесконечно.
                # Срок честно меряется от момента, когда воркер ВЗЯЛСЯ за
                # источник, а не от постановки в очередь: стоять в очереди
                # за своей страной не стыдно, и мёртвым из-за неё источник
                # не считается. Брошенный воркер дорабатывает в фоне, а его
                # конфиги капают в общий зачёт (см. _drain_settled).
                now = time.monotonic()
                if deadline > 0:
                    for fut, (url, t0) in list(pending.items()):
                        started = self._live_started(url)
                        if started is None:
                            # Ещё в очереди: дедлайн ему не указ — очередь не
                            # его вина. Страховка от «голодания» — max_queue_wait:
                            # если все потоки заняты молотильщиками, стоявших
                            # дольше срока пропускаем, дожмут в следующем цикле.
                            over_cap = (s.max_queue_wait_seconds > 0
                                        and now - t0 >= s.max_queue_wait_seconds)
                            if (over_cap or fut.cancelled()) and fut.cancel():
                                pending.pop(fut, None)
                                abandoned.add(fut)
                                counters.done += 1
                                counters.deadlines += 1
                                counters.deadlines_unstarted += 1
                                self.debug.log("Дедлайн", f"{url} — не дождался очереди "
                                                          f"({'превышено ожидание' if over_cap else 'задача снята'}); "
                                                          f"остался в sources.txt, дожмёт в следующем цикле")
                            continue
                        if now - started >= deadline:
                            if fut.cancel():
                                continue  # стартовая гонка — воркер ещё не пометился
                            pending.pop(fut, None)
                            abandoned.add(fut)
                            self._settle_deadline(url, fut, counters, deadline)

                # фоновый добор — в счётчики; метки кулдауна и реабилитации —
                # на диск по расписанию, а не на каждый чих
                self._drain_settled(counters)
                self.bl_book.maybe_flush()
                self._flush_rehabilitations()

                # давно ни один источник не завершается — показываем, сколько
                # их ещё в полёте, и напоминаем про Ctrl+C
                if counters.done != last_done:
                    last_done, last_ts = counters.done, now
                elif now - last_ts > s.stall_warn_seconds:
                    c.warn(f"Нет прогресса {int(now - last_ts)} сек — жду {len(pending)} "
                           f"источников (дедлайн {deadline} сек, Ctrl+C — остановить)")
                    last_ts = now  # если тишина продолжится — напомним снова
                if s.show_progress_bar:
                    self._print_progress(counters, total)
                if self.stop_event.is_set():
                    interrupted = True
                    break
        except KeyboardInterrupt:
            interrupted = True
            self.stop_event.set()
            c.warn("Ctrl+C: останавливаю потоки и сохраняю прогресс...")
        except Exception as exc:
            # неожиданная ошибка не должна уничтожить накопленное: останавливаем
            # потоки, но прогресс и финальную перезапись проводим как обычно
            interrupted = True
            self.stop_event.set()
            c.error(f"Неожиданная ошибка цикла: {exc!r} — сохраняю прогресс и завершаю цикл.")
        finally:
            # зависшие задачи не ждём: не начатые отменяются, результаты
            # тех, кто успел завершиться, забираем в счётчики
            for fut in pending:
                fut.cancel()
            self.http_pool.bump_gen()  # непрочитанные задачи цикла выбрасываются
            for fut in pending:
                if fut.done() and not fut.cancelled() and fut not in abandoned:
                    self._collect(fut, counters)
            # дедлайн-источники: кто уже закончил — окончательный вердикт,
            # кто ещё работает — его текущий добор уже учтён дельтами
            self._drain_settled(counters)
            self.bl_book.flush()
            self._flush_rehabilitations(force=True)
        c.clear_progress()
        gc.collect()  # вернуть отработавшие буферы системе

        # финальная перезапись sources.txt со всеми мёртвыми ссылками
        # (force — мимо дебаунса)
        self._persist_failures(rw_state, force=True)
        cancelled_cnt = total - counters.done
        self._print_summary(counters, cancelled_cnt, total)
        if self.debug.enabled:
            self.debug.log("Цикл", f"финиш: OK {counters.ok}, FAIL {counters.fail}, "
                                   f"дедлайнов {counters.deadlines} (спасено {counters.deadlines_kept}), "
                                   f"в ЧС {len(self._removed)}, кулдаунов {counters.cooldown_skipped}")
        if interrupted:
            c.ok("Прогресс сохранён, возврат в главное меню.")
        return interrupted

    # ------------------------------------------------------------------ конвейер (watch)
    def _sources_snapshot(self) -> tuple[list[str], set[str]]:
        """Читает sources.txt: все строки и множество URL среди них."""
        lines = read_lines_safe(SOURCES_FILE)
        urls: set[str] = set()
        for line in lines:
            cleaned = clean_source_line(line)
            if is_url_line(cleaned):
                urls.add(cleaned)
        return lines, urls

    def run_mini_cycle(self, urls: list[str], threads: int | None = None) -> bool:
        """Мини-цикл конвейера: тот же пул, дедуп и ЧС, но только по новым URL.

        Отличия от полного цикла: без GitHub-фикса (он дорогой и работает
        по всем ссылкам — это дело полных циклов), без самопроверки ЧС и
        возврата кулдаунов, без перечитывания dedupe-хранилища configs.txt
        — оно уже в памяти с запуска конвейера. Мёртвые источники
        отправляются в ЧС тем же механизмом, а sources.txt перечитывается
        с диска под замком, так что ссылки, дописанные Telegram_Checker'ом
        во время мини-цикла, не теряются.

        threads — сколько потоков выделять (None — как в основном цикле):
        настройка conveyor_mini_threads позволяет гонять мини-циклы
        меньшим числом потоков, чтобы не грузить слабый процессор.

        Возвращает True, если пользователь остановил работу (Ctrl+C).
        """
        s, c = self.s, self.c
        proc_urls = list(dict.fromkeys(u for u in urls if u))
        if not proc_urls:
            return False
        interrupted = False
        self.stop_event.clear()
        self.http_pool.ensure_capacity()
        counters = _Counters()
        rw_state: dict[str, Any] = {"n": 0, "ts": 0.0}
        with self._removed_lock:
            self._removed.clear()
        with self._live_lock:
            self._live.clear()
        self._settling.clear()
        self.http_pool.set_max(s.http_threads if not threads else max(1, threads))
        self.http_pool.bump_gen()

        c.header(f"Конвейер: мини-цикл по {len(proc_urls)} новым источникам")
        deadline = max(0, s.source_deadline_seconds)
        window = max(4, s.http_threads * 2)
        urls_iter = iter(proc_urls)
        pending: dict[Future, tuple[str, float]] = {}
        abandoned: set[Future] = set()

        def _submit(url: str) -> None:
            pending[self.http_pool.submit(self._process_source, url)] = (url, time.monotonic())

        try:
            for _ in range(min(window, len(proc_urls))):
                _submit(next(urls_iter))
            while pending:
                done_set, _ = wait(set(pending), timeout=0.2, return_when=FIRST_COMPLETED)
                for fut in done_set:
                    pending.pop(fut, None)
                    if fut not in abandoned:
                        self._collect(fut, counters)
                self._persist_failures(rw_state)
                while not self.stop_event.is_set() and len(pending) < window:
                    try:
                        _submit(next(urls_iter))
                    except StopIteration:
                        break
                now = time.monotonic()
                if deadline > 0:
                    for fut, (url, t0) in list(pending.items()):
                        started = self._live_started(url)
                        if started is None:
                            over_cap = (s.max_queue_wait_seconds > 0
                                        and now - t0 >= s.max_queue_wait_seconds)
                            if (over_cap or fut.cancelled()) and fut.cancel():
                                pending.pop(fut, None)
                                abandoned.add(fut)
                                counters.done += 1
                                counters.deadlines += 1
                                counters.deadlines_unstarted += 1
                                self.debug.log("Дедлайн", f"{url} — не дождался очереди "
                                                          f"(мини-цикл)")
                            continue
                        if now - started >= deadline:
                            if fut.cancel():
                                continue
                            pending.pop(fut, None)
                            abandoned.add(fut)
                            self._settle_deadline(url, fut, counters, deadline)
                self._drain_settled(counters)
                self.bl_book.maybe_flush()
                self._flush_rehabilitations()
                if s.show_progress_bar:
                    self._print_progress(counters, len(proc_urls))
                if self.stop_event.is_set():
                    interrupted = True
                    break
        except KeyboardInterrupt:
            interrupted = True
            self.stop_event.set()
            c.warn("Ctrl+C: останавливаю потоки и сохраняю прогресс...")
        finally:
            for fut in pending:
                fut.cancel()
            self.http_pool.bump_gen()
            for fut in pending:
                if fut.done() and not fut.cancelled() and fut not in abandoned:
                    self._collect(fut, counters)
            self._drain_settled(counters)
            self.bl_book.flush()
            self._flush_rehabilitations(force=True)
        c.clear_progress()
        gc.collect()  # после мини-цикла отдаём отработавшие буферы системе
        self._persist_failures(rw_state, force=True)
        self._print_summary(counters, len(proc_urls) - counters.done, len(proc_urls))
        if self.debug.enabled:
            self.debug.log("Мини-цикл", f"финиш: OK {counters.ok}, FAIL {counters.fail}, "
                                        f"новых конфигов {counters.added}, "
                                        f"в ЧС {len(self._removed)}")
        return interrupted

    def run_watch(self) -> bool:
        """Конвейерная загрузка: круглосуточная работа с мгновенным подхватом.

        Схема работы (каждый шаг настраивается — группа «Конвейерная
        загрузка» в меню настроек):
          1. Полный цикл по всем источникам sources.txt.
          2. Добор перед отдыхом (conveyor_drain_before_rest): пока в
             sources.txt появляются ссылки, которых конвейер ещё не видел,
             по ним идут мини-циклы — всё, что Telegram_Checker и
             telegram_sub_bot успели дописать, пока шёл полный цикл,
             докачивается в configs.txt до ухода в отдых.
          3. Отдых до следующего полного цикла (rest_seconds). Во время
             отдыха каждые conveyor_poll_seconds секунд файл проверяется
             по штампу (mtime + размер), и новые ссылки
             (conveyor_process_during_rest) сразу обрабатываются
             мини-циклом — ждать конца отдыха не нужно. Перед мини-циклом
             ссылки копятся в пачку (conveyor_batch_wait_seconds), чтобы
             не нагружать сеть и диск из-за каждой строчки по отдельности.
          4. После мини-цикла отдых продолжается
             (conveyor_keep_rest_schedule): таймер полного цикла не
             сбрасывается — отдыхается ровно оставшееся время. Если
             настройку выключить, полный цикл отсчитывается заново.

        Новым считается URL, которого нет в _known_sources: ссылки,
        обработанные полным или мини-циклом, а также собственные
        перезаписи файла (github-фикс, чистка ЧС) мини-цикл не поднимают.
        При rest_seconds <= 0 полные циклы идут не чаще раза в 60 сек.
        Возвращает True, если пользователь остановил конвейер (Ctrl+C).
        """
        c, s = self.c, self.s
        self._known_sources = set()
        self._absorb_known_sources()
        c.header("Конвейерная загрузка: полные циклы + мгновенный подхват новых источников")
        c.info(f"Полный цикл каждые {fmt_duration(max(60, s.rest_seconds))}; "
               f"новые источники смотрю каждые {max(3, s.conveyor_poll_seconds)} сек "
               f"(Ctrl+C — остановить конвейер)")
        if not s.conveyor_process_during_rest:
            c.warn("Подхват в отдыхе выключен (conveyor_process_during_rest) — "
                   "новые ссылки доедут только с полным циклом.")
        interrupted = False
        try:
            # стартовый полный цикл — конвейер начинает с актуального
            # содержимого всех подписок; обработанное он сам помечает
            # известным (см. run_cycle), так что добор возьмёт только то,
            # что соседи дописали уже ПОСЛЕ чтения файла циклом
            interrupted = self.run_cycle()
            if not interrupted and s.conveyor_drain_before_rest:
                interrupted = self._drain_before_rest()
            next_full = time.monotonic() + max(60, s.rest_seconds)
            while not interrupted:
                if self._conveyor_rest(next_full) == "stop":
                    break
                # пора полный цикл; после него — снова добор и отдых
                interrupted = self.run_cycle()
                if not interrupted and s.conveyor_drain_before_rest:
                    interrupted = self._drain_before_rest()
                next_full = time.monotonic() + max(60, s.rest_seconds)
        except KeyboardInterrupt:
            interrupted = True
            c.clear_progress()
            c.info("")
            c.warn("Конвейер остановлен пользователем.")
        return interrupted

    # ------------------------------------------------------------------ конвейер: помощники
    def _absorb_known_sources(self) -> None:
        """Запоминает все ссылки текущего sources.txt как «уже известные»."""
        try:
            _, known = self._sources_snapshot()
            self._known_sources |= known
        except Exception:
            pass

    def _fresh_sources(self, mark: bool = True) -> list[str]:
        """Ссылки sources.txt, которых конвейер ещё не видел.

        Известными объявляются только те ссылки, что вернутся вызывающему
        коду: кто не влез из-за лимита мини-цикла (conveyor_max_mini_urls),
        остаётся «новым» и доедет следующим мини-циклом. mark=False только
        подсматривает список, ничего не помечая, — так накопление пачки
        может сколько угодно раз заглядывать в файл без риска потерять
        ссылки: помечаются известными ровно те, что ушли в мини-цикл.
        """
        _, current = self._sources_snapshot()
        fresh = sorted(u for u in current if u not in self._known_sources)
        limit = self.s.conveyor_max_mini_urls
        take = fresh if limit <= 0 else fresh[:limit]
        if mark:
            self._known_sources |= set(take)
        return take

    def _drain_before_rest(self) -> bool:
        """Добор новых источников перед отдыхом — до исчерпания или лимита раундов.

        Telegram_Checker и telegram_sub_bot дописывают ссылки, пока полный
        цикл ещё качает; без добора они ждали бы конца отдыха и следующего
        полного прохода — именно эту задержку конвейер и убирает.
        Раунд — чтение sources.txt и мини-цикл по свежим ссылкам; между
        раундами короткая пауза, чтобы дозаписи соседей успели попасть
        в файл.
        Возвращает True, если пользователь остановил работу (Ctrl+C).
        """
        s, c = self.s, self.c
        rounds = max(1, s.conveyor_drain_max_rounds)
        poll = max(3, s.conveyor_poll_seconds)
        for round_no in range(1, rounds + 1):
            if self.stop_event.is_set():
                break
            fresh = self._fresh_sources()
            if not fresh:
                break
            c.info(f"Добор перед отдыхом (раунд {round_no}/{rounds}): "
                   f"новых источников {len(fresh)}")
            if self.run_mini_cycle(fresh):
                return True
            if round_no < rounds:
                time.sleep(min(poll, 15))  # дать соседям дописать хвост
        return False

    def _conveyor_rest(self, next_full: float) -> str:
        """Отдых конвейера до полного цикла: опрос sources.txt и подхват.

        Каждые conveyor_poll_seconds секунд файл проверяется по штампу;
        если появились ссылки, которых конвейер не видел, собирается пачка
        (conveyor_batch_wait_seconds) и запускается мини-цикл прямо во
        время отдыха. Пока пачка копится, ссылки НЕ помечаются
        известными: в мини-цикл уходит всё накопленное разом (вместе с
        тем, что соседи дописали за время накопления), и помечается
        только отправленное — потеряться не может ничего. Таймер полного
        цикла (next_full) при этом не
        трогается: после мини-цикла отдыхается оставшееся время
        (conveyor_keep_rest_schedule); если настройка выключена — полный
        цикл отсчитывается заново.

        Возвращает "full" (пора полный цикл) или "stop" (Ctrl+C).
        """
        s, c = self.s, self.c
        poll = max(3, int(s.conveyor_poll_seconds))
        last_poll = time.monotonic()
        last_tick = 0.0
        picked = 0  # сколько новых источников подхвачено за этот отдых
        last_sig: tuple[int, int] | None = None
        try:
            st = SOURCES_FILE.stat()
            last_sig = (st.st_size, st.st_mtime_ns)
        except OSError:
            pass
        while True:
            now = time.monotonic()
            if now >= next_full:
                c.clear_progress()
                return "full"
            # живая строка отдыха: сколько осталось и сколько подхвачено
            if now - last_tick >= 1.0:
                last_tick = now
                remain = max(0, int(next_full - now))
                c.progress(c.paint(
                    f"Конвейер | отдых до полного цикла: {_hms(remain)} "
                    f"| подхвачено за отдых: {picked} "
                    f"(Ctrl+C — остановить)", "gray"))
            time.sleep(min(0.5, max(0.05, next_full - now)))
            if time.monotonic() - last_poll < poll:
                continue
            last_poll = time.monotonic()
            sig: tuple[int, int] | None = None
            try:
                st = SOURCES_FILE.stat()
                sig = (st.st_size, st.st_mtime_ns)
            except OSError:
                pass
            if sig is None or sig == last_sig:
                continue
            last_sig = sig
            if not s.conveyor_process_during_rest:
                continue  # подхват выключен — новые доедут с полным циклом
            fresh = self._fresh_sources(mark=False)
            if not fresh:
                continue
            # пачка: соседи дописывают ссылки по одной — даём им время
            # собраться. Ссылки, замеченные до и во время накопления,
            # уезжают в один мини-цикл; до самой отправки они не помечаются
            # известными, так что пропасть не могут ни при каком раскладе
            batch: set[str] = set(fresh)
            if s.conveyor_batch_wait_seconds > 0:
                c.info(f"Конвейер: в sources.txt добавлено источников: {len(fresh)} — "
                       f"собираю пачку {fmt_duration(s.conveyor_batch_wait_seconds)}...")
                deadline = time.monotonic() + s.conveyor_batch_wait_seconds
                while time.monotonic() < deadline:
                    time.sleep(0.25)
                batch |= set(self._fresh_sources(mark=False))
            else:
                c.info(f"Конвейер: в sources.txt добавлено источников: {len(fresh)}")
            if not batch:
                continue
            proc = sorted(batch)
            limit = s.conveyor_max_mini_urls
            take = proc if limit <= 0 else proc[:limit]
            self._known_sources |= set(take)
            picked += len(take)
            threads = s.conveyor_mini_threads or s.http_threads
            if self.run_mini_cycle(take, threads=threads):
                return "stop"
            if not s.conveyor_keep_rest_schedule:
                # отдых после мини-цикла отсчитывается заново
                next_full = time.monotonic() + max(60, s.rest_seconds)
            # иначе: таймер не трогаем — отдыхается оставшееся время

    # ------------------------------------------------------------------ sources.txt из интернета
    def _ensure_sources_file(self) -> None:
        """Гарантирует, что sources.txt есть и в нём есть ссылки.

        Если файла нет или в нём не осталось ни одной URL-строки, а
        настройка auto_download_sources включена — тянем резервный список
        из sources_fallback_url. Существующее содержимое (заметки,
        комментарии) не выбрасываем: ссылки дописываются к нему. Так
        свежая установка или случайно удалённый файл не оставляют
        программу без работы.
        """
        s = self.s
        if not s.auto_download_sources:
            return
        if SOURCES_FILE.exists() and any(is_url_line(ln) for ln in iter_file_lines(SOURCES_FILE)):
            return
        c = self.c
        c.info("sources.txt пуст или отсутствует — скачиваю список источников из интернета...")

        def _show_download_progress(done: int, total: int) -> None:
            """Прогресс-бар скачивания списка источников: проценты по Content-Length.

            Если сервер не прислал размер, проценты посчитать не на чем —
            показываем просто, сколько килобайт уже принято.
            """
            if total > 0:
                pct = min(100, done * 100 // total)
                c.progress(f"{c.bar(done, total)} "
                           f"{c.paint(str(pct) + '%', c.accent, bold=True)} | "
                           f"{done // 1024} / {total // 1024} КБ")
            else:
                c.progress(f"{c.paint('Список источников:', 'gray')} "
                           f"принято {done // 1024} КБ")

        res = fetch_url(
            s.sources_fallback_url, user_agent=s.user_agent,
            timeout_connect=s.timeout_normal, timeout_read=s.timeout_read,
            max_mb=s.max_response_mb, verify=s.verify_ssl,
            on_progress=_show_download_progress,
        )
        c.clear_progress()
        if not res.ok:
            c.warn(f"Список источников скачать не удалось: {res.error}")
            self.debug.log("Источники", f"резервный список не скачался: {res.error}")
            return
        urls: list[str] = []
        seen: set[str] = set()
        for line in res.text.splitlines():
            url = clean_source_line(line)
            if is_url_line(url) and url not in seen:
                seen.add(url)
                urls.append(url)
        if not urls:
            # Бывает: по ссылке живёт заглушка вроде «потом». Не страшно.
            c.warn("В скачанном списке не оказалось ссылок — sources.txt не трогаю.")
            self.debug.log("Источники", "в резервном списке нет ссылок")
            return
        with _AdvisoryLock(SOURCES_LOCK_FILE, console=self.c, name="sources.txt"):
            try:
                existing_lines = read_lines_safe(SOURCES_FILE) if SOURCES_FILE.exists() else []
                known = {clean_source_line(ln) for ln in existing_lines}
                merged = existing_lines + [u for u in urls if u not in known]
                write_lines(SOURCES_FILE, merged)
                c.ok(f"Список источников загружен: {len(urls)} ссылок "
                     f"(в sources.txt теперь {len(merged)} строк)")
                self.debug.log("Источники", f"резервный список: получено {len(urls)} ссылок, "
                                            f"дописано {len(merged) - len(existing_lines)}")
            except OSError as exc:
                c.error(f"Не удалось записать sources.txt: {exc}")

    # ------------------------------------------------------------------ дедлайн
    def _settle_deadline(self, url: str, fut: Future, counters: _Counters,
                         deadline: int) -> None:
        """Источник не уложился в дедлайн — перестаём ждать, но не хороним.

        Воркер продолжает работу в фоне: дочитает ответ (лимит времени
        скачивания его всё равно остановит), вынет конфиги и запишет их.
        Всё, что он принесёт, уйдёт в счётчики через _drain_settled —
        по дельтам из таблицы живости, поэтому «новых» в прогрессе и
        прирост configs.txt всегда совпадают. Чёрный список здесь не
        трогается: жив источник или мёртв — воркер сам разберётся по
        итогам своей работы, с конфигами он или без.
        """
        counters.done += 1
        counters.deadlines += 1
        live_found, live_added, _size, _busy = self._live_get(url)
        # База — нули: всё, что источник успел найти к этому моменту и
        # найдёт дальше, _drain_settled целиком досчитает в счётчики.
        # Если бы базой стало текущее live_found, найденное до дедлайна
        # навсегда выпало бы из «Всего конфигураций/Новых»: брошенная
        # задача в _collect больше не попадает и второй раз не учтётся.
        self._settling[fut] = [url, 0, 0]
        self.c.debug(f"Дедлайн {deadline} сек — результат догонит в фоне: {_short_url(url)}")
        self.debug.log("Дедлайн", f"{url} — превысил {deadline} сек, воркер оставлен "
                                  f"дорабатывать в фоне (конфигов на момент метки: {live_found})")

    def _drain_settled(self, counters: _Counters) -> None:
        """Сводит фоновый добор дедлайн-источников с общими счётчиками.

        Для каждого дедлайн-источника берём из таблицы живости, сколько он
        успел найти и записать, и добавляем в счётчики только дельту — так
        статистика растёт синхронно с файлом, а не одним куском в конце.
        Кто уже закончил — получает окончательный вердикт (с конфигами —
        зачёт, без — чёрный список по выбранному методу) и вылетает из реестра.
        """
        for fut, entry in list(self._settling.items()):
            url = entry[0]
            live_found, live_added, _size, _busy = self._live_get(url)
            d_found = live_found - entry[1]
            d_added = live_added - entry[2]
            if d_found > 0:
                counters.found += d_found
                entry[1] += d_found
            if d_added > 0:
                counters.added += d_added
                entry[2] += d_added
            if not fut.done():
                continue
            del self._settling[fut]
            try:
                res = fut.result()
            except Exception as exc:
                counters.fail += 1
                self.debug.log("Дедлайн", f"{url} — фоновый воркер упал: {exc!r}")
                continue
            if res.cancelled:
                continue
            if res.ok:
                counters.ok += 1
                counters.deadlines_kept += 1
                counters.decoded += res.decoded
                counters.happ += res.via_happ
                for proto, n in res.protocols.items():
                    counters.protocols[proto] = counters.protocols.get(proto, 0) + n
                if res.bl_action == "rehabilitated":
                    counters.rehabilitated += 1
                self.c.debug(f"Фоновый источник догнал — найдено {res.found}, "
                             f"новых {res.added}: {_short_url(url)}")
                self.debug.log("Дедлайн", f"{url} — фоновый воркер закончил: "
                                          f"найдено {res.found}, новых {res.added}; зачтён")
            else:
                counters.fail += 1
                if res.bl_action == "blacklisted":
                    with self._removed_lock:
                        self._removed.add(url)
                elif res.bl_action == "cooldown":
                    counters.cooldown_skipped += 1
                self.debug.log("Дедлайн", f"{url} — фоновый воркер закончил провалом "
                                          f"({res.error}); чёрный список: {res.bl_action}")

    # ------------------------------------------------------------------ чёрный список / sources.txt
    def _update_sources_file(self, keep, extra_lines: list[str] = (),
                             context: str = "") -> bool:
        """Безопасная перезапись sources.txt при параллельных писателях.

        Читает свежий файл (его могли дополнить Telegram_Checker или
        telegram_sub_bot, пока шёл цикл — раньше такие строки терялись),
        оставляет строки, для которых keep(line, url) вернул True, и
        дописывает extra_lines, которых ещё нет. Всё — под advisory-замком
        sources.lock: дозапись соседа между нашим чтением и записью на
        диск не пропадает.

        keep(line, url): url — вычищенная ссылка строки или None для
        текста/комментария (текст не трогаем, как и в run_cycle).
        """
        with _AdvisoryLock(SOURCES_LOCK_FILE, console=self.c, name="sources.txt"):
            fresh = read_lines_safe(SOURCES_FILE)
            kept: list[str] = []
            for line in fresh:
                cleaned = clean_source_line(line)
                url = cleaned if is_url_line(cleaned) else None
                if keep(line, url):
                    kept.append(line)
            # Дедупликация extra_lines — против УЖЕ ОСТАВЛЕННЫХ строк, а не
            # против fresh: extra_lines у github-фикса — это ПОЛНЫЙ
            # обновлённый список (неисправленные ссылки в нём повторяют
            # исходные строки), а keep-предикат как раз вычеркнул их из kept
            # как «снимок». Сверка с fresh навсегда блокировала бы их
            # возврат — sources.txt терял все источники после замены хотя
            # бы одной raw-ссылки. Сверка с kept возвращает всё, кроме
            # вычеркнутых.
            present = set(kept)
            for line in extra_lines:
                if line and line not in present:
                    kept.append(line)
                    present.add(line)
            try:
                write_lines(SOURCES_FILE, kept)
                return True
            except OSError as exc:
                # Повторять одну и ту же ошибку каждые несколько секунд —
                # только засорять экран: ругаемся один раз, дальше — в журнал.
                if "sources.txt" not in self._warned_writes:
                    self._warned_writes.add("sources.txt")
                    self.c.error(f"Не удалось обновить sources.txt: {exc} — попробую позже")
                self.debug.log("Файлы",
                               f"sources.txt не перезаписан ({context or 'без метки'}): {exc!r}")
                return False

    def _persist_failures(self, state: dict[str, Any], force: bool = False) -> None:
        """Вычёркивает отправленные в ЧС ссылки из sources.txt по ходу цикла.

        Файл переписывается атомарно, но не на каждый сбой: тридцать
        подряд умерших ссылок должны обернуться одной записью, а не
        тридцатью, поэтому между записями выдерживается пауза
        _REWRITE_DEBOUNCE. force=True — «записать сейчас, не дожидаясь
        паузы», так закрывается цикл. state помнит, сколько сбоев уже
        записано (n) и когда была последняя запись (ts).

        В self._removed попадают только те, кого реально отправили в ЧС:
        отложенные по кулдауну остаются в sources.txt до следующего раза.
        Файл перечитывается с диска под замком (см. _update_sources_file):
        ссылки, дописанные другими скриптами во время цикла, не теряются.
        """
        with self._removed_lock:
            removed_total = len(self._removed)
        if removed_total == 0 or removed_total <= state["n"]:
            return
        now = time.monotonic()
        if not force and now - state["ts"] < _REWRITE_DEBOUNCE:
            return  # до следующей записи подождём паузу или конец цикла
        with self._removed_lock:
            removed = set(self._removed)
        if not self._update_sources_file(
                lambda line, url: url is None or url not in removed,
                context="чёрный список"):
            return
        delta = removed_total - state["n"]
        state["n"] = removed_total
        state["ts"] = now
        # Сообщения про ЧС — тихие: серым в подробном режиме и в журнал.
        self.c.debug(f"Чёрный список: из sources.txt удалено источников: {delta} "
                     f"(всего за цикл: {state['n']})")
        self.debug.log("ЧС", f"sources.txt обновлён: удалено {delta}, всего за цикл {state['n']}")

    # ------------------------------------------------------------------ воркер
    def _process_source(self, url: str) -> SourceResult:
        """Воркер: скачать один источник и вынуть из него все конфиги.

        Ответ разбирается потоком, найденные конфиги уходят в configs.txt
        пачками — большой текст не копится в памяти. Если прямое чтение
        ничего не дало, делается второй заход через Happ, и только после
        него источник признаётся пустым или мёртвым. Снятие по Ctrl+C
        провалом не считается. Каждый шаг — сетевой, парсерский, вердикт —
        отражается в отладочном журнале, когда тот включён.
        """
        s = self.s
        dlog = self.debug
        if self.stop_event.is_set():
            return SourceResult(url=url, ok=False, cancelled=True)
        dlog.log("Источник", f"начал обработку: {url}")
        self._live_start(url)  # с этого момента дедлайн знает: источник в работе
        attempts = max(1, s.source_attempts)
        res = FetchResult(False, error="запрос не выполнялся")
        for attempt in range(1, attempts + 1):
            if self.stop_event.is_set():
                dlog.log("Итог", f"{url} — снят по Ctrl+C до ответа")
                return SourceResult(url=url, ok=False, cancelled=True)
            res = fetch_url(
                url, user_agent=s.user_agent, timeout_connect=s.timeout_normal,
                timeout_read=s.timeout_read, max_mb=s.max_response_mb, verify=s.verify_ssl,
                on_progress=lambda n, _t, _u=url: self._live_size(_u, n),
                max_seconds=s.max_download_seconds,
            )
            if res.ok:
                dlog.log("Сеть", f"{url} — попытка {attempt}: HTTP {res.status}, {res.size} байт"
                                 + (f"; {res.note}" if res.note else ""))
            else:
                dlog.log("Сеть", f"{url} — попытка {attempt}: сбой ({res.error})")
            # Повторяем только то, что может пройти со второй попытки:
            # таймауты, обрывы, серверные 5xx. Честный 403/404 — нет.
            if res.ok or not _retryable(res) or attempt == attempts:
                break
            pause = min(3, max(1, s.timeout_normal))
            dlog.log("Сеть", f"{url} — переспрашиваю через {pause} сек "
                             f"({attempt + 1}/{attempts})")
            time.sleep(pause)

        error = res.error
        found = added = decoded = via_happ = 0
        protocols: dict[str, int] = {}
        if res.ok:
            # разбор и запись идут потоком, пачками — configs.txt
            # пополняется ещё до конца чтения источника
            stats: dict[str, Any] = {"decoded": 0}
            found, added, protocols = self._consume(iter_extract_from_content(res.text, s, stats), url)
            decoded = stats.get("decoded", 0)
            dlog.log("Парсер", f"{url} — методы: {_fmt_methods(_method_summary(stats))}")
            res.text = ""  # большой ответ больше не нужен — освобождаем сразу
        # Разбор первого ответа закончен: дальше дедлайн не спасает источник
        # «размером ответа» — данные либо дали конфиги, либо нет. Это важно
        # для висящих Happ-заходов: пустой источник с зависшим фолбэком
        # должен честно уехать в ЧС, а не вечно держаться за размер ответа.
        self._live_parse_done(url)
        if found == 0 and s.decrypt_happ and not self.stop_event.is_set():
            happ = self._fetch_happ(url)
            if happ.ok:
                dlog.log("Happ", f"{url} — фолбэк: HTTP {happ.status}, {happ.size} байт"
                                 + (", ответ похож на HTML — пропущен" if looks_like_html(happ.text) else ""))
            else:
                dlog.log("Happ", f"{url} — фолбэк не удался: {happ.error}")
            if happ.ok and not looks_like_html(happ.text):
                stats = {"decoded": 0}
                found, added, protocols = self._consume(iter_extract_from_content(happ.text, s, stats), url)
                decoded = stats.get("decoded", 0)
                via_happ = found
        if found == 0:
            if self.stop_event.is_set():
                # источник сняли по Ctrl+C — это не его вина: в чёрный
                # список не отправляем, в sources.txt оставляем
                dlog.log("Итог", f"{url} — снят по Ctrl+C, вердикта нет")
                return SourceResult(url=url, ok=False, cancelled=True)
            reason = error or "конфигурации не найдены"
            action = self._register_failure(url, reason)
            dlog.log("Итог", f"{url} — провал ({reason}); чёрный список: {action}")
            return SourceResult(url=url, ok=False, error=reason, bl_action=action)
        bl_action = self._rehabilitate(url)
        if bl_action:
            dlog.log("Итог", f"{url} — числился в чёрном списке, но заработал; метка снята")
        dlog.log("Итог", f"{url} — найдено {found}, новых {added}"
                         + (f", через Happ {via_happ}" if via_happ else ""))
        return SourceResult(url=url, ok=True, found=found, added=added,
                            protocols=protocols, decoded=decoded, via_happ=via_happ,
                            bl_action=bl_action)

    def _consume(self, urls: Iterator[str], url: str = "") -> tuple[int, int, dict[str, int]]:
        """Принимает конфиги из парсера и пишет их в файл пачками.

        Пачка — _ADD_BATCH штук: configs.txt обновляется ещё во время
        скачивания, и при остановке цикла на диске остаётся всё уже
        найденное. url — чей поток конфигов: на каждом найденном и каждой
        записанной пачке воркер отмечается в таблице _live, чтобы
        дедлайн-контроль видел, что источник живой. Возвращает
        (найдено всего, дописано новых, счётчики по протоколам).
        """
        found = added = 0
        protocols: dict[str, int] = {}
        batch: list[str] = []
        while not self.stop_event.is_set():
            try:
                cfg = next(urls)
            except StopIteration:
                break
            except Exception as exc:
                # сломанный генератор продолжению не подлежит; падение
                # фиксируем в журнале — молча терять конфиги недопустимо
                self.debug.log("Парсер", f"{url or 'поток конфигов'} — разбор оборван ошибкой: "
                                         f"{type(exc).__name__}: {exc}")
                break
            found += 1
            if url:
                self._live_note(url, configs=1)
            proto = urlsplit(cfg).scheme.lower()
            if proto:
                protocols[proto] = protocols.get(proto, 0) + 1
            if self._protocol_enabled(cfg):
                batch.append(cfg)
            if len(batch) >= _ADD_BATCH:
                got = self.store.add(batch)
                added += got
                batch.clear()
                if url and got:
                    self._live_note(url, added=got)
        if batch:
            got = self.store.add(batch)
            added += got
            if url and got:
                self._live_note(url, added=got)
        return found, added, protocols

    def _fetch_happ(self, url: str) -> FetchResult:
        """Второй заход по Happ: та же ссылка, но с параметром hwid.

        Часть раздающих смотрит на User-Agent и параметр hwid и без них
        отдаёт заглушку вместо списка. Если в настройках сгенерировано
        устройство (меню «Сгенерировать X-HWID») — заход изображает его:
        HWID в параметре ?hwid=, согласованный User-Agent и заголовки
        X-HWID/X-Device-* (их понимают панели Remnawave). Пока устройство
        не сгенерировано (по умолчанию) — HWID нигде не вшит: заход идёт
        только с User-Agent Happ, без параметра и заголовков. Оба таймаута
        короткие: резервный заход не должен удваивать ожидание на
        медленном источнике.
        """
        s = self.s
        device_headers: dict[str, str] = {}
        if s.happ_hwid:
            hwid = s.happ_hwid
            ua = happ_user_agent(s)
            device_headers = {
                "X-HWID": hwid,
                "X-Device-OS": "Android",
                "X-Ver-OS": s.happ_device_os,
                "X-Device-Model": s.happ_device_model,
                "X-App-Version": s.happ_app_version or DEFAULT_HAPP_VERSION,
            }
            device_headers = {k: v for k, v in device_headers.items() if v}
            separator = "&" if "?" in url else "?"
            happ_url = f"{url}{separator}hwid={hwid}"
        else:
            ua = happ_user_agent(s)
            happ_url = url
        return fetch_url(happ_url, user_agent=ua, timeout_connect=s.timeout_happ,
                         timeout_read=s.timeout_happ, max_mb=s.max_response_mb,
                         verify=s.verify_ssl, extra_headers=device_headers or None)

    def _protocol_enabled(self, url: str) -> bool:
        """Разрешают ли настройки сохранять конфиги этой схемы."""
        key = _SCHEME_TO_TOGGLE.get(urlsplit(url).scheme.lower())
        return key is None or bool(getattr(self.s, key))

    # ------------------------------------------------------------------ чёрный список: решения
    def _register_failure(self, url: str, reason: str) -> str:
        """Провалившийся источник проходит здесь — ЧС или отсрочка.

        Возвращает, что с ним сделали:
          blacklisted — отправлен в ЧС (и помечен временем для кулдауна);
          cooldown    — в режиме «Кулдаун» его туда недавно уже отправляли:
                        пусть пока остаётся в sources.txt и пробует снова;
          disabled    — отправка в ЧС отключена настройкой вовсе.
        """
        s = self.s
        if not s.save_failed_sources:
            return "disabled"
        if s.blacklist_mode == "cooldown" \
                and self.bl_book.on_cooldown(url, s.blacklist_cooldown_hours):
            self.debug.log("ЧС", f"{url} — отправка отложена: кулдаун "
                                 f"{s.blacklist_cooldown_hours} ч ещё не истёк ({reason})")
            return "cooldown"
        self.bl_book.mark(url)
        with self._bl_lock:
            if url not in self._bl_seen:
                self._bl_seen.add(url)
                try:
                    BLACKLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
                    with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                        f.write(url + "\n")
                        f.flush()
                except OSError:
                    pass
        self.debug.log("ЧС", f"{url} — отправлен в чёрный список ({reason})")
        return "blacklisted"

    def _rehabilitate(self, url: str) -> str:
        """Источник из чёрного списка заработал — вычёркиваем его из ЧС.

        ЧС в режиме «Кулдаун» рассчитан на то, что источники со временем
        оживают. Источник ожил и отдал конфиги — значит запись о нём устарела.
        Метку в памяти снимаем сразу, а файл перезаписываем одной пачкой:
        вычёркивать по одной строке из файла на пятьдесят тысяч — значит
        нагружать диск на каждом ожившем источнике.
        """
        with self._bl_lock:
            if url not in self._bl_seen:
                return ""
            self._bl_seen.discard(url)
        with self._rehab_lock:
            self._rehab_pending.add(url)
        self.bl_book.clear(url)
        return "rehabilitated"

    def _flush_rehabilitations(self, force: bool = False) -> None:
        """Вычёркивает реабилитированных из файла ЧС одной записью.

        По ходу цикла файл трогаем только если накопилось много (от
        тысячи), в конце цикла — всегда: самопроверка следующего цикла
        читает этот файл, и она не должна видеть снятых меток. Чтение и
        запись — под _bl_lock, тем же, каким _register_failure дописывает
        в файл новых мёртвых: иначе дозапись, вклинившаяся между чтением
        и перезаписью, затёрлась бы, и строка ЧС терялась.
        """
        with self._rehab_lock:
            if not self._rehab_pending:
                return
            if not force and len(self._rehab_pending) < 1000:
                return
            pending = set(self._rehab_pending)
            self._rehab_pending.clear()
        with self._bl_lock:
            kept = [ln for ln in iter_file_lines(BLACKLIST_FILE)
                    if ln.strip() and ln.strip() not in pending]
            try:
                write_lines(BLACKLIST_FILE, kept)
            except OSError:
                return  # метки в памяти уже сняты — файл подтянется позже
            # файл теперь без pending — и память о нём приводим в соответствие
            self._bl_seen.difference_update(pending)
        self.debug.log("ЧС", f"из blacklist_sources.txt вычеркнуто реабилитированных: {len(pending)}")

    def _migrate_blacklist_to_sources(self) -> None:
        """Возвращает файл ЧС на штатное место: sources/blacklist_sources.txt.

        Одна из прошлых версий вела список в temp/blacklist_sources.txt,
        а Telegram_Checker читает sources/blacklist_sources.txt — из-за
        расхождения связка ЧС не работала: чекер продолжал принимать
        ссылки, которые этот скрипт уже похоронил. Теперь файл один, в
        sources/, а копия из temp/ при старте:
          • в sources/ файла ещё нет — переезжает туда целиком;
          • существуют оба — построчно с дедупом дописывается в sources/,
            и temp-копия удаляется: содержимое уже в основном файле,
            а два файла гарантируют путаницу в будущем.
        Вызывается один раз при старте, до любых чтений и записей ЧС.
        """
        legacy = TEMP_DIR / "blacklist_sources.txt"
        try:
            if not legacy.exists():
                return
            legacy_urls = [ln.strip() for ln in read_lines_safe(legacy) if ln.strip()]
            if not BLACKLIST_FILE.exists():
                shutil.move(str(legacy), str(BLACKLIST_FILE))
                self.c.debug("Чёрный список перенесён в sources/blacklist_sources.txt")
                self.debug.log("ЧС", f"файл ЧС переехал: {legacy} -> {BLACKLIST_FILE}")
                return
            known = {ln.strip() for ln in iter_file_lines(BLACKLIST_FILE) if ln.strip()}
            merged = [url for url in legacy_urls if url not in known]
            if merged:
                with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
                    f.write("\n".join(merged) + "\n")
                self.c.info(f"В чёрный список слито из temp-копии: {len(merged)} ссылок")
            self.debug.log("ЧС", f"temp-копия ЧС слита в {BLACKLIST_FILE}: "
                                 f"перенесено {len(merged)}, всего {len(known) + len(merged)}")
            legacy.unlink()  # содержимое переехало целиком — дубль не нужен
        except OSError as exc:
            self.c.warn(f"Не удалось перенести чёрный список из temp/: {exc}")

    # ------------------------------------------------------------------ вывод
    def _collect(self, fut: Future[SourceResult], counters: _Counters) -> None:
        """Забирает результат готовой задачи в счётчики цикла.

        Вызывается только из главного потока, поэтому счётчики без
        замков. Упавшая задача — просто ещё один неудачный источник,
        а не повод останавливать обработку.
        """
        if fut.cancelled():
            return
        try:
            res = fut.result()
        except Exception as exc:  # задача упала сама — считаем её сбоем
            res = SourceResult(url="", ok=False, error=f"worker: {exc!r}")
        if res.cancelled:
            return
        counters.done += 1
        if res.ok:
            counters.ok += 1
            if res.bl_action == "rehabilitated":
                counters.rehabilitated += 1
        else:
            counters.fail += 1
            if res.bl_action == "blacklisted" and res.url:
                with self._removed_lock:
                    self._removed.add(res.url)
            elif res.bl_action == "cooldown":
                counters.cooldown_skipped += 1
        counters.added += res.added
        counters.found += res.found
        counters.decoded += res.decoded
        counters.happ += res.via_happ
        for proto, n in res.protocols.items():
            counters.protocols[proto] = counters.protocols.get(proto, 0) + n
        if self.s.log_level == "verbose" and res.url:
            mark = "OK  " if res.ok else "FAIL"
            bl_tag = _BL_ACTION_LABELS.get(res.bl_action, "")
            self.c.verbose(f"{mark} {res.url} найдено={res.found} новых={res.added} "
                           f"{bl_tag} {res.error}".rstrip(), ok=res.ok)

    def _print_progress(self, counters: _Counters, total: int) -> None:
        """Живая однострочная сводка: бар, процент, OK/FAIL, протоколы.

        Печатается поверх предыдущей строки (\r), обычный вывод не
        портится. В узкий терминал целиком не влезает — сокращается
        ступеньками: сначала уходит статистика по протоколам, потом
        общий счётчик найденного.
        """
        c, p = self.c, counters.protocols
        pct = 100 if total <= 0 else min(100, counters.done * 100 // total)
        bar = c.bar(counters.done, total)
        stats = (f"VLESS: {p.get('vless', 0)} | VMess: {p.get('vmess', 0)} | "
                 f"Trojan: {p.get('trojan', 0)} | SS: {p.get('ss', 0)} | "
                 f"Hysteria2: {p.get('hysteria2', 0)} | TUIC: {p.get('tuic', 0)} | "
                 f"Декодировано: {counters.decoded} | "
                 f"HWID: {counters.happ} | Новых: {counters.added} | "
                 f"В файле: {self.store.total}")
        okfail = (f"{c.paint('OK: ' + str(counters.ok), 'green')}/"
                  f"{c.paint('FAIL: ' + str(counters.fail), 'red')}")
        frac = 1.0 if total <= 0 else min(1.0, counters.done / max(1, total))
        filled = int(round(frac * 20))
        plain_bar = "|" + "█" * filled + "░" * (20 - filled) + "|"
        full_plain = (f"{plain_bar} {pct}% | OK: {counters.ok}/FAIL: {counters.fail} "
                      f"| {stats}")
        # ширину меряем по неокрашенной копии строки: ANSI-коды место
        # в терминале не занимают, а в len() — занимают
        width = shutil.get_terminal_size((120, 30)).columns
        if len(full_plain) <= width - 2:
            self.c.progress(f"{bar} {c.paint(str(pct) + '%', c.accent, bold=True)} "
                            f"| {okfail} | {stats}")
            return
        compact = (f"{bar} {pct}% | OK: {counters.ok}/FAIL: {counters.fail} "
                   f"| Конфигов: {counters.found} | Новых: {counters.added}")
        compact_plain = (f"{plain_bar} {pct}% | OK: {counters.ok}/FAIL: {counters.fail} "
                         f"| Конфигов: {counters.found} | Новых: {counters.added}")
        if len(compact_plain) <= width - 2:
            self.c.progress(compact)
        else:
            self.c.progress(f"{bar} {pct}% | OK: {counters.ok}/FAIL: {counters.fail} "
                            f"| Новых: {counters.added}")

    def _print_summary(self, counters: _Counters, cancelled: int, total: int) -> None:
        """Итоговая таблица после обработки всех источников."""
        c, p = self.c, counters.protocols
        c.header("Итог обработки подписок:")
        ok_part = c.paint(f"Успешные подписки: {counters.ok}", "green", bold=True)
        fail_part = c.paint(f"Неудачные подписки: {counters.fail}", "red", bold=True)
        tail = ""
        if counters.deadlines:
            tail = f" (брошено по дедлайну: {counters.deadlines}"
            if counters.deadlines_kept:
                tail += f", из них с конфигами — зачтены: {counters.deadlines_kept}"
            if counters.deadlines_unstarted:
                tail += f", не начавших из очереди: {counters.deadlines_unstarted}"
            tail += ")"
        if cancelled:
            tail += f" (отменено: {cancelled})"
        c.always(f"{ok_part} | {fail_part}{tail}")
        if self._settling:
            c.always(f"Ещё качают в фоне: {len(self._settling)} — их конфиги дописываются в файл")
        c.always(f"Всего конфигураций: {counters.found}")
        c.always(f"VLESS: {p.get('vless', 0)} | VMess: {p.get('vmess', 0)} | "
                 f"Trojan: {p.get('trojan', 0)} | SS: {p.get('ss', 0)} | "
                 f"Hysteria2: {p.get('hysteria2', 0)} | TUIC: {p.get('tuic', 0)}")
        c.always(f"Декодировано: {counters.decoded}")
        c.always(f"Обработано через Happ метод: {counters.happ}")
        c.always(f"Добавлено новых в файл: {counters.added}")
        c.always(f"Всего конфигов в файле: {self.store.total}")
        with self._removed_lock:
            removed_total = len(self._removed)
        if self.s.save_failed_sources and removed_total:
            c.always(f"В чёрный список добавлено источников: {removed_total}")
        if counters.cooldown_skipped:
            c.always(f"Отправка в ЧС отложена (кулдаун): {counters.cooldown_skipped}")
        if counters.rehabilitated:
            c.always(f"Убрано из чёрного списка (заработали): {counters.rehabilitated}")
        if self.s.show_memory_usage:
            mem = get_memory_mb()
            if mem is not None:
                c.always(f"Использовано памяти: {mem:.1f} МБ")
        c.always("")


def _method_summary(stats: dict[str, Any]) -> dict[str, int]:
    """Оставляет из парсерской статистики только ненулевые методы добычи."""
    out: dict[str, int] = {}
    for key in ("direct", "json", "yaml"):
        if stats.get(key):
            out[key] = stats[key]
    for key in sorted(k for k in stats if k.startswith("b64_")):
        if stats[key]:
            out[key] = stats[key]
    return out


def _fmt_methods(methods: dict[str, int]) -> str:
    """«прямые: 12, base64 ур.1: 340, yaml: 5» — читаемо для журнала."""
    names = {"direct": "прямые", "json": "json", "yaml": "yaml"}
    parts = []
    for key, val in methods.items():
        name = names.get(key) or key.replace("b64_", "base64 ур.")
        parts.append(f"{name}: {val}")
    return ", ".join(parts) if parts else "ничего"


# ══════════════════════════════════════════════════════════════════════════════
# ИНТЕРФЕЙС: логотип, главное меню, меню настроек
# ══════════════════════════════════════════════════════════════════════════════

_DEFAULT_LOGO: str = r"""
   _  _     ____  _  __ _____
 _| || |_  |  _ \| |/ /|  _  |
|_  ..  _| | |_) | ' / | | | |
|_      _| |  __/| . \ | | | |
  |_||_|   |_|   |_|\_\|_| |_|
         Downloader.py
"""


def _load_logo() -> str:
    """Логотип: сначала banner.txt рядом со скриптом, затем встроенный."""
    banner = ROOT_DIR / "banner.txt"
    try:
        if banner.exists():
            text = banner.read_text(encoding="utf-8-sig", errors="replace").rstrip("\r\n")
            if text.strip():
                return text
    except OSError:
        pass
    return _DEFAULT_LOGO.rstrip("\n")


LOGO: str = _load_logo()

# Глифы расшифровки: только ASCII — наборы символов терминалов Windows
# различаются, а спецэффект не должен рассыпаться на квадратики.
_DECODE_GLYPHS = "01<>*+=#$%&@?/\\|"

_LOGO_ANIMATE = True   # первый показ логотипа — с анимацией расшифровки


def _logo_codes(accent: str) -> tuple[str, str]:
    """Коды акцентного цвета для анимации: (закреплённый, перебираемый).

    COLOR_CODES хранит яркие варианты (90–97); приглушённый получается
    переводом в обычную интенсивность (30–37) вычитанием 60 из кода.
    Анимация красится тем же акцентом, что и статичный логотип.
    """
    bright = COLOR_CODES.get(accent, COLOR_CODES["blue"])
    dim = "\033[" + str(int(bright[2:-1]) - 60) + "m"
    return _BOLD + bright, dim


def _logo_decode(console: Console, text: str) -> None:
    """Логотип «расшифровывается»: символы перебираются и закрепляются волной.

    Время закрепления клетки растёт слева направо и сверху вниз, поэтому
    логотип проявляется диагональной волной. Кадры перерисовывают блок
    логотипа на месте: курсор поднимается на высоту блока. Закреплённые
    символы красятся как статичный логотип (акцент + жирный), поэтому
    последний кадр с ним совпадает.
    """
    on_code, dim = _logo_codes(console._accent)
    lines = text.split("\n")
    out = sys.stdout
    trail = max(len(l) for l in lines) // 2
    with console._lock:
        try:
            out.write("\033[?25l")
            for t in range(len(lines) + trail + 2):
                if t:
                    time.sleep(0.03)
                    out.write(f"\033[{len(lines)}A")
                buf = []
                for r, line in enumerate(lines):
                    row = []
                    for c, ch in enumerate(line):
                        if ch == " ":
                            row.append(" ")
                        elif t >= r + c // 2:
                            row.append(on_code + ch)
                        else:
                            row.append(dim + random.choice(_DECODE_GLYPHS))
                    buf.append("".join(row) + _RESET + "\033[K")
                out.write("\n".join(buf) + "\n")
                out.flush()
        finally:
            out.write("\033[?25h")
            out.flush()


def _logo_first_show(console: Console, text: str) -> bool:
    """Первый показ логотипа — через анимацию расшифровки.

    Анимация возможна, когда вывод — терминал с включёнными цветами
    (use_colors уже учитывает isatty); сбой не роняет запуск — логотип
    печатается статично. Возвращает True, если анимация отрисовала
    логотип сама.
    """
    global _LOGO_ANIMATE
    animate = _LOGO_ANIMATE
    _LOGO_ANIMATE = False
    if not (animate and console.use_colors):
        return False
    try:
        _logo_decode(console, text)
        return True
    except (KeyboardInterrupt, Exception):
        return False

class _AppLike(Protocol):
    settings: Settings
    console: Console
    downloader: Downloader

    def apply_output_settings(self) -> None: ...


def _ask(prompt: str) -> str | None:
    """Ввод, устойчивый к Ctrl+C/EOF: None — сигнал «назад/отмена»."""
    try:
        return input(prompt)
    except (KeyboardInterrupt, EOFError):
        print()
        return None


def _fmt_param(spec: ParamSpec, value: Any) -> str:
    """Значение параметра для консоли — всегда по-русски и по-человечески.

    Время — «2 часа», выбор — русская подпись («Обычный»), булево —
    «Да/Нет». Английские служебные значения (verbose, always) остаются
    внутри настроек, в JSON, а на экран их тащить незачем. Длинные строки
    обрезаются до 60 символов, кроме тех, что пользователь просил
    показывать целиком (ссылка на список источников и User-Agent).
    """
    if spec.kind == "duration":
        if value == 0 and spec.zero_means:
            return spec.zero_means
        return fmt_duration(int(value))
    if spec.kind == "int" and value == 0 and spec.zero_means:
        return spec.zero_means
    if spec.unit == "hours" and isinstance(value, int) and not isinstance(value, bool):
        return fmt_hours(value)
    if spec.kind == "choice" and spec.labels and spec.choices and value in spec.choices:
        return spec.labels[spec.choices.index(value)]
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    if isinstance(value, list):
        preview = ", ".join(str(x) for x in value[:2])
        suffix = f" … (+{len(value) - 2})" if len(value) > 2 else ""
        return f"[{preview}{suffix}] ({len(value)} шт.)"
    text = str(value)
    show_full = spec.key in ("sources_fallback_url", "user_agent")
    if not show_full and len(text) > 60:
        text = text[:57] + "..."
    return f'"{text}"' if isinstance(value, str) else text


def _param_value(app: _AppLike, spec: ParamSpec) -> str:
    """Строка значения для меню.

    Обычные параметры берутся из настроек; информационные (kind='info')
    вычисляются на месте — например, User-Agent Happ собирается из
    нескольких полей и в настройках не хранится.
    """
    if spec.kind == "info" and spec.key == "happ_ua_display":
        return f'"{happ_user_agent(app.settings)}"'
    return _fmt_param(spec, getattr(app.settings, spec.key))


def _persist(app: _AppLike) -> bool:
    try:
        save_settings(SETTINGS_FILE, app.settings)
        app.apply_output_settings()
        return True
    except OSError as exc:
        app.console.always(app.console.paint(f"  Не удалось сохранить настройки: {exc}", "red"))
        return False


# --------------------------------------------------------------------- запуск циклов
def _run_cycles(app: _AppLike, single: bool = False) -> None:
    """Циклы загрузки. single=True — один проход и возврат в главное меню."""
    c = app.console
    while True:
        interrupted = False
        try:
            interrupted = app.downloader.run_cycle()
        except KeyboardInterrupt:
            c.warn("Цикл прерван пользователем — прогресс сохранён.")
            interrupted = True
        if interrupted:
            return
        rest = app.settings.rest_seconds
        if single or rest <= 0:
            c.info("Цикл завершён.")
            _ask("Нажмите Enter для возврата в меню...")
            return
        c.info(f"Цикл завершён.")
        c.always(c.paint(f"Новый цикл начнётся через {fmt_duration(rest)}.", c.accent))
        if not _countdown(app, rest):
            return


def _countdown(app: _AppLike, seconds: int) -> bool:
    """Живой обратный отсчёт до следующего цикла; False — Ctrl+C (в меню)."""
    c = app.console
    end = time.monotonic() + max(0, seconds)
    try:
        last: int | None = None
        while True:
            remain = round(end - time.monotonic())
            if remain <= 0:
                c.clear_progress()
                return True
            if remain != last:
                last = remain
                c.progress(c.paint(f"До следующего цикла: {_hms(remain)}", "gray"))
            time.sleep(0.2)
    except KeyboardInterrupt:
        c.clear_progress()
        c.info("")
        return False


def _hms(seconds: int) -> str:
    minutes, sec = divmod(max(0, seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{sec:02d}"


def _goodbye(console: Console) -> None:
    console.info("Завершение работы...")
    time.sleep(1)


# --------------------------------------------------------------------- действия меню

def _print_happ_device(app: _AppLike) -> None:
    """Показать User-Agent'ы и устройство Happ, которые сейчас в настройках."""
    c = app.console
    s = app.settings
    c.always()
    c.always(c.paint("  User-Agent для обычных загрузок:", c.accent))
    c.always(f"    {s.user_agent}")
    if s.happ_hwid:
        c.always(c.paint("  Сгенерированное устройство Happ (сохранено в настройках):", c.accent))
        c.always(f"    User-Agent: {happ_user_agent(s)}")
        c.always(f"    X-HWID: {s.happ_hwid}")
        c.always(f"    Модель (X-Device-Model): {s.happ_device_model or '—'}")
        c.always(f"    Версия ОС (X-Ver-OS): {s.happ_device_os or '—'}")
        c.always(f"    Версия Happ (X-App-Version): {s.happ_app_version or DEFAULT_HAPP_VERSION}")
    else:
        c.always(c.paint("  X-HWID не сгенерирован — Happ-заход идёт без HWID "
                         "(в коде вшитого нет).", "yellow"))


def _action_generate_hwid(app: _AppLike) -> None:
    """Пункт «Сгенерировать X-HWID и устройство»: новое устройство
    раскладывается по настройкам и сохраняется — оно используется,
    пока пользователь его не удалит или не сгенерирует заново."""
    c = app.console
    device = generate_happ_device()
    s = app.settings
    s.happ_hwid = device["hwid"]
    s.happ_device_model = device["model"]
    s.happ_device_os = device["os"]
    s.happ_app_version = device["app"]
    s.happ_ua_number = device["ua_number"]
    if _persist(app):
        c.always()
        c.always(c.paint("  X-HWID и устройство сгенерированы и сохранены в настройки.", "green"))
        _print_happ_device(app)
    _ask("  Нажмите Enter для возврата...")


def _action_clear_hwid(app: _AppLike) -> None:
    """Пункт «Удалить X-HWID и устройство»: очистка с подтверждением."""
    c = app.console
    s = app.settings
    if not s.happ_hwid:
        c.always(c.paint("  X-HWID и так не задан — удалять нечего.", "yellow"))
        _ask("  Нажмите Enter для возврата...")
        return
    _print_happ_device(app)
    c.always()
    c.always("  Удалить сгенерированные X-HWID и устройство? (Да/Нет):")
    answer = _ask("  > ")
    if answer is None or answer.strip().lower() not in ("да", "д", "y", "yes", "1", "+"):
        c.always(c.paint("  Отменено.", "yellow"))
        return
    s.happ_hwid = s.happ_device_model = s.happ_device_os = ""
    s.happ_app_version = s.happ_ua_number = ""
    if _persist(app):
        c.always(c.paint("  X-HWID и устройство удалены — Happ-заход снова без HWID.", "green"))


def _run_menu_action(app: _AppLike, spec: ParamSpec) -> None:
    if spec.key == "action_generate_hwid":
        _action_generate_hwid(app)
    elif spec.key == "action_clear_hwid":
        _action_clear_hwid(app)


# --------------------------------------------------------------------- правка параметров
def _edit_param(app: _AppLike, spec: ParamSpec) -> None:
    """Диалог правки одного параметра.

    Переключатели Да/Нет срабатывают сразу при выборе пункта — ввод не
    нужен. Остальное правило ввода: понимать можно что угодно — русское,
    английское, число, номер варианта. Правило показа: на экране только
    русское значение.
    """
    c = app.console
    if spec.kind == "action":
        _run_menu_action(app, spec)
        return
    if spec.kind == "info":
        # только показ: значение показывается целиком, редактировать нечего
        c.always()
        c.always(f"  {spec.title}:")
        c.always(f"    {_param_value(app, spec)}")
        _ask("  Нажмите Enter для возврата...")
        return
    current = getattr(app.settings, spec.key)
    if spec.kind == "bool":
        # переключатель: Да становится Нет и наоборот, сохраняем сразу
        value = not bool(current)
        setattr(app.settings, spec.key, value)
        if _persist(app):
            c.always(c.paint(f"  {spec.title}: {_fmt_param(spec, current)} → "
                             f"{_fmt_param(spec, value)}", "green"))
        return
    c.always()
    c.always(f"  Параметр: {spec.title}")
    c.always(f"  Текущее значение: {c.paint(_param_value(app, spec), c.accent)}")

    value: Any = None
    if spec.kind == "int":
        if spec.key == "blacklist_cooldown_hours":
            c.always(f"  Введите число часов от {spec.minimum} до {spec.maximum} "
                     f"(или пустую строку для отмены):")
        elif spec.minimum is not None and spec.maximum is not None:
            c.always(f"  Введите целое число от {spec.minimum} до {spec.maximum} "
                     f"(или пустую строку для отмены):")
        else:
            c.always("  Введите целое число (или пустую строку для отмены):")
        raw = _ask("  > ")
        if raw is None:
            return
        if not raw.strip():
            c.always(c.paint("  Отменено.", "yellow"))
            return
        try:
            value = int(raw.strip())
        except ValueError:
            c.always(c.paint("  Нужно ввести целое число.", "yellow"))
            return
        # сравнение именно с None, а не «or»: ноль — законное значение,
        # и клампинг для него отключать нельзя
        lo = spec.minimum if spec.minimum is not None else value
        hi = spec.maximum if spec.maximum is not None else value
        clamped = max(lo, min(hi, value))
        if clamped != value:
            c.always(c.paint(f"  Значение скорректировано до диапазона: {clamped}", "yellow"))
            value = clamped
    elif spec.kind == "duration":
        lo = fmt_duration(spec.minimum) if spec.minimum else "0"
        hi = fmt_duration(spec.maximum or 0)
        c.always(f"  Введите длительность от {lo} до {hi} (пустая строка — отмена).")
        c.always("  Формат: 7200 — секунды, либо с суффиксом: 2ч, 1ч30м, 45м, 90с (латиницей: 2h, 30m, 45s).")
        raw = _ask("  > ")
        if raw is None:
            return
        if not raw.strip():
            c.always(c.paint("  Отменено.", "yellow"))
            return
        seconds = parse_duration_input(raw)
        if seconds is None:
            c.always(c.paint("  Непонятная длительность. Примеры: 2ч, 1ч30м, 45м, 90с, 7200.", "yellow"))
            return
        lo_s = spec.minimum if spec.minimum is not None else seconds
        hi_s = spec.maximum if spec.maximum is not None else seconds
        clamped = max(lo_s, min(hi_s, seconds))
        if clamped != seconds:
            c.always(c.paint(f"  Значение скорректировано до диапазона: {fmt_duration(clamped)}", "yellow"))
        value = clamped
    elif spec.kind == "choice":
        options = list(spec.choices or ())
        labels = list(spec.labels or ())
        for i, opt in enumerate(options, 1):
            name = labels[i - 1] if i - 1 < len(labels) else opt
            c.always(f"    {i}. {name}")
        c.always("  Введите номер или английское значение (verbose/always/...), пустая строка — отмена:")
        raw = _ask("  > ")
        if raw is None:
            return
        if not raw.strip():
            c.always(c.paint("  Отменено.", "yellow"))
            return
        text = raw.strip().lower()
        if text.isdigit() and 1 <= int(text) <= len(options):
            value = options[int(text) - 1]
        elif text in options:
            value = text
        elif text in labels:  # с русской раскладки можно и словом
            value = options[labels.index(text)]
        else:
            c.always(c.paint("  Некорректный выбор.", "yellow"))
            return
    elif spec.kind == "str":
        c.always("  Введите новое значение (или пустую строку для отмены):")
        raw = _ask("  > ")
        if raw is None:
            return
        value = raw.strip()
        if not value:
            c.always(c.paint("  Отменено.", "yellow"))
            return
    else:
        return

    setattr(app.settings, spec.key, value)
    if _persist(app):
        c.always(c.paint(f"  Сохранено: {_fmt_param(spec, value)}", "green"))


def _category_menu(app: _AppLike, category: CategorySpec) -> None:
    c = app.console
    while True:
        if app.settings.clear_screen:
            c.clear_screen()
        c.banner(LOGO)
        c.always()
        c.header(f"  === {category.title} ===")
        c.always()
        for i, spec in enumerate(category.params, 1):
            if spec.kind == "action":
                c.always(c.paint(f"  {i}. {spec.title}", c.accent))
            else:
                value = c.paint(_param_value(app, spec), c.accent)
                c.always(f"  {i}. {spec.title}: {value}")
        c.always()
        c.always(c.paint("  0. Назад", "gray"))
        c.always()
        choice = _ask("  Выберите номер параметра (0 — назад): ")
        if choice is None:
            return
        text = choice.strip()
        if text == "0":
            return
        if text == "":
            continue
        if text.isdigit() and 1 <= int(text) <= len(category.params):
            _edit_param(app, category.params[int(text) - 1])
        elif text.isdigit():
            c.always(c.paint("  Нет такого параметра.", "yellow"))
        else:
            c.always(c.paint("  Нужно ввести число.", "yellow"))


def _reset_settings(app: _AppLike) -> None:
    c = app.console
    c.always()
    c.always("  Сбросить ВСЕ настройки к значениям по умолчанию? (Да/Нет):")
    answer = _ask("  > ")
    if answer is None or answer.strip().lower() not in ("да", "д", "y", "yes", "1", "+"):
        c.always(c.paint("  Отменено.", "yellow"))
        return
    copy_defaults(app.settings)
    if _persist(app):
        c.always(c.paint("  Настройки сброшены к значениям по умолчанию.", "green"))


def _settings_menu(app: _AppLike) -> None:
    c = app.console
    while True:
        if app.settings.clear_screen:
            c.clear_screen()
        c.banner(LOGO)
        c.always()
        c.header("  === Настройки ===")
        c.always()
        for i, category in enumerate(CATEGORIES, 1):
            c.always(c.paint(f"  {i}. {category.title}", c.accent))
        c.always()
        c.always(c.paint("  0. Назад", "gray"))
        c.always(c.paint("  r. Сбросить значения по умолчанию", "gray"))
        c.always()
        choice = _ask("  Выберите номер группы: ")
        if choice is None:
            return
        text = choice.strip()
        if text == "0":
            return
        if text == "":
            continue
        # «r» сбрасывает настройки; «к» — та же клавиша на русской раскладке
        if text.lower() in ("r", "к"):
            _reset_settings(app)
        elif text.isdigit() and 1 <= int(text) <= len(CATEGORIES):
            _category_menu(app, CATEGORIES[int(text) - 1])
        elif text.isdigit():
            c.always(c.paint("  Нет такой группы.", "yellow"))
        else:
            c.always(c.paint("  Нужно ввести число.", "yellow"))


# --------------------------------------------------------------------- главное меню
def run_menu(app: _AppLike) -> None:
    c = app.console
    first_show = True
    while True:
        if app.settings.clear_screen:
            c.clear_screen()
        c.banner(LOGO)
        c.always()
        if first_show:
            time.sleep(1)  # пауза после логотипа — сделано автором намеренно
            first_show = False
        c.header("══════════ Главное меню ══════════")
        c.always(" 1) Запустить цикл загрузки (автоповтор)")
        c.always(" 2) Конвейерная загрузка (автоповтор: полные циклы + "
                 "мгновенный подхват новых)")
        c.always(" 3) Одиночный цикл")
        c.always(" 4) Настройки")
        c.always(" 5) Выход")
        c.always()
        choice = _ask("Выберите пункт (1-5): ")
        if choice is None:
            _goodbye(c)
            return
        text = choice.strip()
        if text == "1":
            _run_cycles(app, single=False)
        elif text == "2":
            _run_watch_mode(app)
        elif text == "3":
            _run_cycles(app, single=True)
        elif text == "4":
            _settings_menu(app)
        elif text == "5":
            _goodbye(c)
            return
        else:
            c.warn("Неизвестный пункт, повторите ввод.")


def _run_watch_mode(app: _AppLike) -> None:
    """Пункт меню «Конвейерная загрузка»: круглосуточный конвейер до
    Ctrl+C, затем возврат в меню."""
    c = app.console
    try:
        app.downloader.run_watch()
    except KeyboardInterrupt:
        c.info("")
        c.warn("Конвейер остановлен пользователем.")
    c.info("Возврат в главное меню.")


# ══════════════════════════════════════════════════════════════════════════════
# ПРИЛОЖЕНИЕ И ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════════════════════

class App:
    """Контейнер приложения: настройки, консоль, отладочный журнал, загрузчик."""

    def __init__(self, force_debug: bool = False) -> None:
        ensure_dirs()
        if not SETTINGS_FILE.exists():
            save_settings(SETTINGS_FILE, Settings())
        # Настройки от прошлой версии: разовая подгонка старых
        # умолчаний под текущие (см. retune_legacy_data).
        try:
            legacy_raw = json.loads(SETTINGS_FILE.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            legacy_raw = None
        self.settings: Settings = load_settings(SETTINGS_FILE)
        retuned = retune_legacy_data(legacy_raw) if isinstance(legacy_raw, dict) else []
        if retuned:
            for key, _old, new in retuned:
                setattr(self.settings, key, new)
            try:
                save_settings(SETTINGS_FILE, self.settings)
            except OSError:
                pass  # не сохранилось — значения всё равно применены на этот запуск
        if force_debug:
            self.settings.debug_logs = True  # в JSON не пишем: только на этот запуск
        self.console: Console = Console(
            self.settings.log_level, self.settings.use_colors, self.settings.accent_color
        )
        for key, old, new in retuned:
            self.console.info(f"Настройка «{key}»: {old} → {new} "
                              f"(умолчания обновлены в этой версии)")
        self.debug_log: DebugLog = DebugLog()
        if self.settings.debug_logs:
            self.debug_log.enable()
        self._faulthandler_file: Any = None
        self._setup_faulthandler()
        self.downloader: Downloader = Downloader(self.settings, self.console, self.debug_log)

    def _setup_faulthandler(self) -> None:
        if not self.settings.enable_faulthandler:
            return
        try:
            import faulthandler
            self._faulthandler_file = open(
                DOWNLOADER_DIR / "faulthandler.log", "a", encoding="utf-8", buffering=1
            )
            faulthandler.enable(self._faulthandler_file)
        except Exception:
            self._faulthandler_file = None

    def apply_output_settings(self) -> None:
        """Применяет настройки вывода «на лету» (меню вызывает после изменений)."""
        self.console.set_level(self.settings.log_level)
        self.console.set_colors(self.settings.use_colors)
        self.console.set_accent(self.settings.accent_color)
        # журнал — тоже настройка вывода: включили в меню, он тут же открылся
        if self.settings.debug_logs and not self.debug_log.enabled:
            self.debug_log.enable()
        elif not self.settings.debug_logs and self.debug_log.enabled:
            self.debug_log.disable()

    def shutdown(self) -> None:
        """Финальные записи на диск при выходе: метки кулдауна и реабилитации.

        Память и файл расходятся, только если программа умерла насильственно;
        при штатном выходе всё дописывается, чтобы следующий запуск увидел
        ровно то, что насчитал этот.
        """
        try:
            self.downloader.bl_book.flush()
            self.downloader._flush_rehabilitations(force=True)
        except Exception:
            pass  # на прощании уже не до бухгалтерии


def main() -> int:
    parser = argparse.ArgumentParser(description="Загрузчик и парсер прокси-конфигураций")
    parser.add_argument("--once", action="store_true", help="выполнить один цикл и выйти")
    parser.add_argument("--watch", action="store_true",
                        help="конвейерная загрузка: полные циклы по расписанию, "
                             "добор новых источников перед отдыхом и мгновенный "
                             "подхват подписок прямо в отдыхе (круглосуточно)")
    parser.add_argument("--debug", action="store_true",
                        help="включить подробный журнал logs/Downloader на этот запуск")
    args = parser.parse_args()

    try:
        app = App(force_debug=args.debug)
    except OSError as exc:
        print(f"Критическая ошибка инициализации (нет прав на запись?): {exc}", file=sys.stderr)
        return 2

    if args.watch:
        try:
            app.downloader.run_watch()
        except KeyboardInterrupt:
            app.console.warn("Прервано пользователем — прогресс сохранён.")
        app.shutdown()
        return 0

    if args.once:
        try:
            app.downloader.run_cycle()
        except KeyboardInterrupt:
            app.console.warn("Прервано пользователем — прогресс сохранён.")
        app.shutdown()
        # --once зовут из планировщиков: если файла источников так и не
        # появилось — это авария настройки, а не тихий нулевой выход.
        # Файл мог создаться внутри цикла (автоскачивание), поэтому
        # проверяем после цикла.
        if not SOURCES_FILE.exists():
            app.console.error("Источников нет: sources.txt не существует.")
            return 2
        return 0

    try:
        run_menu(app)
    except KeyboardInterrupt:
        app.console.always("")
        app.console.error("Принудительное прерывание — выход.")
        time.sleep(1)
    app.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
