#!/usr/bin/env python3
"""
Сборщик бесплатных VPN-подписок (чёрные и белые списки).

Вход:
  free_sources.txt — по одной строке: black|https://... или white|https://...
                     строки с # игнорируются
Флаги:
  --discover   сначала поиск новых источников на GitHub по месяцам
               (нужен GITHUB_TOKEN для нормального лимита)

Выход:
  results.txt            — все уникальные конфиги
  results_black.txt.gz   — чёрный список (для парсера)
  results_white.txt.gz   — белый список (для парсера)
  results_stats.json     — сводка

Что делает: скачивает, извлекает ссылки (включая base64), проверяет синтаксис,
убирает дубликаты, распределяет по категориям. Реальную проверку работоспособности
делает парсер (sing-box), здесь она была бы слишком тяжёлой для миллионов ссылок.
"""

import base64
import gzip
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from urllib.parse import urlsplit, parse_qs

import requests

SOURCES_FILE = "free_sources.txt"
OUT_ALL = "results.txt"
OUT_BLACK = "results_black.txt.gz"
OUT_WHITE = "results_white.txt.gz"
OUT_STATS = "results_stats.json"

TIMEOUT = 10
MAX_BYTES = 20 * 1024 * 1024
WORKERS = 64
DISCOVER_MAX_REPOS_TOTAL = 300   # общий лимит новых репозиториев за один запуск
DISCOVER_WORKERS = 8             # параллельные запросы дерева файлов
USER_AGENT = "Mozilla/5.0 (compatible; free-collector/1.0)"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()

PROTOS = ("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://", "tuic://")
RE_URI = re.compile(r'(?:vless|vmess|trojan|ss|hysteria2|hy2|tuic)://[^\s"\'<>]+', re.I)

# поиск на GitHub: месяцы за последние N лет
DISCOVER_QUERIES = [
    ("black", "vless free vpn subscription"),
    ("black", "v2ray free configs"),
    ("black", "vmess trojan free servers"),
    ("white", "white list vpn configs"),
    ("white", "whitelist vless subscription"),
]
DISCOVER_MONTHS = 24
DISCOVER_PAGES = 1          # 1 страница = до 100 репозиториев на запрос
DISCOVER_FILES_PER_REPO = 3
WHITE_KEYWORDS = ("white", "whitelist", "wl", "bwl")


# ---------- извлечение и проверка ----------

def decode_maybe_b64(text):
    try:
        raw = text.strip().replace("\n", "")
        return base64.b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def extract(text):
    found = set(m.group(0) for m in RE_URI.finditer(text))
    if not found:
        dec = decode_maybe_b64(text)
        if dec and any(p in dec for p in PROTOS):
            found = set(m.group(0) for m in RE_URI.finditer(dec))
    return found


def valid(uri):
    """Проверка синтаксиса без сетевых запросов."""
    try:
        if uri.lower().startswith("vmess://"):
            raw = decode_maybe_b64(uri[8:])
            v = json.loads(raw)
            return bool(v.get("add")) and bool(v.get("id")) and str(v.get("port", "")).isdigit()
        u = urlsplit(uri)
        if not u.hostname or " " in u.netloc:
            return False
        if u.port is not None and not (1 <= u.port <= 65535):
            return False
        if uri.lower().startswith(("vless://", "trojan://", "hysteria2://", "hy2://", "tuic://")):
            return bool(u.username)
        if uri.lower().startswith("ss://"):
            return True
        return True
    except Exception:
        return False


def key_of(uri):
    return uri.split("#", 1)[0].strip()


# ---------- загрузка ----------

def fetch(url):
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}, stream=True)
        r.raise_for_status()
        data = b""
        for chunk in r.iter_content(65536):
            data += chunk
            if len(data) > MAX_BYTES:
                break
        return data.decode("utf-8", errors="ignore"), None
    except Exception as e:
        return "", f"{type(e).__name__}: {str(e)[:80]}"


def read_sources():
    items = []
    with open(SOURCES_FILE, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or "|" not in s:
                continue
            cat, url = s.split("|", 1)
            cat = cat.strip().lower()
            if cat in ("black", "white") and url.strip().startswith("http"):
                items.append((cat, url.strip()))
    return items


# ---------- поиск на GitHub ----------

def gh_headers():
    h = {"User-Agent": USER_AGENT, "Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def gh_get(url, params=None):
    for _ in range(3):
        try:
            r = requests.get(url, headers=gh_headers(), params=params, timeout=TIMEOUT)
        except Exception:
            time.sleep(5)
            continue
        if r.status_code in (403, 429):
            wait = int(r.headers.get("Retry-After", "60"))
            print(f"  лимит GitHub, жду {wait} с")
            time.sleep(min(wait, 900))
            continue
        return r.json() if r.status_code == 200 else None
    return None


def month_ranges(months):
    today = date.today()
    out = []
    y, m = today.year, today.month
    for _ in range(months):
        start = date(y, m, 1)
        nm = date(y + (m == 12), (m % 12) + 1, 1)
        end = nm - timedelta(days=1)
        out.append((start.isoformat(), end.isoformat()))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return out


def discover_old():
    """Старая версия (не используется)."""
    pause = 1.0 if GITHUB_TOKEN else 7.0
    found = {}
    for cat_hint, q in DISCOVER_QUERIES:
        for start, end in month_ranges(DISCOVER_MONTHS):
            query = f"{q} pushed:{start}..{end}"
            for page in range(1, DISCOVER_PAGES + 1):
                data = gh_get("https://api.github.com/search/repositories",
                              {"q": query, "per_page": 100, "page": page})
                time.sleep(pause)
                if not data or not data.get("items"):
                    break
                for repo in data["items"]:
                    full = repo["full_name"]
                    text_hint = (full + " " + (repo.get("description") or "")).lower()
                    cat = "white" if any(k in text_hint for k in WHITE_KEYWORDS) else "black"
                    branch = repo.get("default_branch", "main")
                    tree = gh_get(f"https://api.github.com/repos/{full}/git/trees/{branch}")
                    time.sleep(pause)
                    if not tree:
                        continue
                    picked = [i["path"] for i in tree.get("tree", [])
                              if i.get("type") == "blob" and
                              (i["path"].lower().endswith(".txt") or i["path"].lower().endswith("readme.md"))]
                    for path in picked[:DISCOVER_FILES_PER_REPO]:
                        url = f"https://raw.githubusercontent.com/{full}/{branch}/{path}"
                        found[url] = cat
            print(f"  поиск {query[:60]} → источников всего {len(found)}")
    return found


def known_repos():
    """Репозитории, уже есть в free_sources.txt (owner/repo)."""
    repos = set()
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, encoding="utf-8") as f:
            for line in f:
                if "|" not in line or line.strip().startswith("#"):
                    continue
                url = line.split("|", 1)[1].strip()
                m = re.match(r"https://raw\.githubusercontent\.com/([^/]+/[^/]+)/", url)
                if m:
                    repos.add(m.group(1))
    return repos


def repo_files(repo, cat):
    """Текстовые файлы одного репозитория (один запрос дерева)."""
    full = repo["full_name"]
    branch = repo.get("default_branch", "main")
    tree = gh_get(f"https://api.github.com/repos/{full}/git/trees/{branch}")
    if not tree:
        return []
    picked = [i["path"] for i in tree.get("tree", [])
              if i.get("type") == "blob" and
              (i["path"].lower().endswith(".txt") or i["path"].lower().endswith("readme.md"))]
    return [(f"https://raw.githubusercontent.com/{full}/{branch}/{p}", cat)
            for p in picked[:DISCOVER_FILES_PER_REPO]]


def discover():
    """Поиск новых репозиториев по месяцам. Каждый репозиторий обрабатывается один раз."""
    search_pause = 2.0 if GITHUB_TOKEN else 7.0
    seen = known_repos()
    found = {}
    budget = DISCOVER_MAX_REPOS_TOTAL
    months = month_ranges(DISCOVER_MONTHS)

    for _cat_hint, q in DISCOVER_QUERIES:
        for start, end in months:
            if budget <= 0:
                break
            query = f"{q} pushed:{start}..{end}"
            data = gh_get("https://api.github.com/search/repositories",
                          {"q": query, "per_page": 30, "page": 1})
            time.sleep(search_pause)
            if not data:
                continue
            new = []
            for repo in data.get("items", []):
                full = repo["full_name"]
                if full in seen:
                    continue
                seen.add(full)
                hint = (full + " " + (repo.get("description") or "")).lower()
                cat = "white" if any(k in hint for k in WHITE_KEYWORDS) else "black"
                new.append((repo, cat))
            new = new[:budget]
            budget -= len(new)

            with ThreadPoolExecutor(max_workers=DISCOVER_WORKERS) as ex:
                futs = [ex.submit(repo_files, repo, cat) for repo, cat in new]
                for fut in as_completed(futs):
                    for url, cat in fut.result():
                        found[url] = cat
            print(f"  {query[:50]} → новых репо {len(new)}, источников всего {len(found)}")
        if budget <= 0:
            print("Достигнут лимит DISCOVER_MAX_REPOS_TOTAL")
            break
    return found


def append_sources(found):
    existing = set()
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, encoding="utf-8") as f:
            for l in f:
                if "|" in l and not l.strip().startswith("#"):
                    existing.add(l.split("|", 1)[1].strip())
    fresh = [(c, u) for u, c in found.items() if u not in existing]
    if fresh:
        with open(SOURCES_FILE, "a", encoding="utf-8") as f:
            for c, u in fresh:
                f.write(f"{c}|{u}\n")
    print(f"Новых источников добавлено: {len(fresh)}")


# ---------- основной сбор ----------

def main():
    if "--discover" in sys.argv:
        append_sources(discover())

    if not os.path.exists(SOURCES_FILE):
        print(f"Нет файла {SOURCES_FILE}")
        sys.exit(1)
    sources = list(dict.fromkeys(read_sources()))
    print(f"Источников: {len(sources)} "
          f"(black {sum(1 for c, _ in sources if c == 'black')}, "
          f"white {sum(1 for c, _ in sources if c == 'white')})")

    t0 = time.time()
    store = {}   # ключ -> {"raw": str, "cats": set}
    ok = fail = 0
    per_cat = {"black": 0, "white": 0}

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch, url): (cat, url) for cat, url in sources}
        for i, fut in enumerate(as_completed(futures), 1):
            cat, url = futures[fut]
            text, err = fut.result()
            if err:
                fail += 1
                continue
            ok += 1
            for uri in extract(text):
                if not valid(uri):
                    continue
                k = key_of(uri)
                rec = store.setdefault(k, {"raw": uri, "cats": set()})
                rec["cats"].add(cat)
                per_cat[cat] += 1
            if i % 50 == 0 or i == len(futures):
                print(f"  {i}/{len(futures)} | уникальных {len(store)}")

    black, white = [], []
    for rec in store.values():
        if "black" in rec["cats"]:
            black.append(rec["raw"])
        if "white" in rec["cats"]:
            white.append(rec["raw"])

    with open(OUT_ALL, "w", encoding="utf-8") as f:
        f.write("\n".join(r["raw"] for r in store.values()) + "\n")
    with gzip.open(OUT_BLACK, "wt", encoding="utf-8") as f:
        f.write("\n".join(sorted(black)) + "\n")
    with gzip.open(OUT_WHITE, "wt", encoding="utf-8") as f:
        f.write("\n".join(sorted(white)) + "\n")

    stats = {
        "sources_ok": ok,
        "sources_failed": fail,
        "unique_total": len(store),
        "black": len(black),
        "white": len(white),
        "seconds": round(time.time() - t0),
        "updated": int(time.time()),
    }
    with open(OUT_STATS, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print("─" * 50)
    print(f"Источников ок {ok}, с ошибкой {fail}")
    print(f"Уникальных: {len(store)} | чёрных {len(black)} | белых {len(white)}")
    print(f"Файлы: {OUT_ALL}, {OUT_BLACK}, {OUT_WHITE}")
    print(f"Время: {stats['seconds']} с")


if __name__ == "__main__":
    main()
