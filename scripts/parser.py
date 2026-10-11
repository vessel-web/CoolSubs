#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import urllib.request
import urllib.error
import base64
import json
import socket
import ssl
import os
import re
import time
import random
import subprocess
import concurrent.futures
import requests
import yaml
from urllib.parse import urlparse, quote
from collections import Counter

BLACK_SOURCES = [
    ("Gidroksi",    "https://gidroksi.fun/black-list"),
    ("Aetris",      "https://gitverse.ru/api/repos/flaafix/AetrisVPN_Black_list/raw/branch/master/configs.txt"),
    ("etoneya",     "https://etoskam.ru/other"),
    ("igareck",     "https://raw.githack.com/igareck/vpn-configs-for-russia/main/BLACK_VLESS_RUS_mobile.txt"),
    ("Diversan",    "https://raw.githubusercontent.com/Diversan313/apex-parser/main/subs/main/alive_bl.txt"),
    ("luxxuria",    "https://github.com/luxxuria/harvester/raw/refs/heads/main/non_ru.txt"),
    ("Akres",       "https://hub.mos.ru/akres/vpn/-/raw/main/all"),
    ("VLESSFORU",   "https://sub.vlessfo.ru/vlessforu/working_configs.txt"),
    ("RKP",         "https://hub.mos.ru/rkp/sub-roskompozor/raw/main/bl"),
    ("Pizduk-sub",  "https://gitverse.ru/api/repos/Pizduk/PizdukVPN/raw/branch/master/sub.txt"),
    ("LimeVPN",     "https://raw.githubusercontent.com/LimeHi/LimeVPN/refs/heads/main/blacklist.txt"),
    ("atbPars",     "https://raw.githubusercontent.com/djsigggmagg-ui/atbPars-sub/refs/heads/main/subscription.txt"),
    ("WarpGen",     "https://warp-gen.cyb-portal.org/CP-039"),
    ("BUNKER",      "https://gitverse.ru/api/repos/KOT_ANTIDOT/BUNKER/raw/branch/master/BUNKER_BLACK700.txt"),
    ("ImSketch",    "https://raw.githubusercontent.com/ImSketch1337/vless-/refs/heads/main/BLWLservers.txt"),
    ("LSO-WIFI",    "https://raw.githubusercontent.com/LSO-LinSpisokObhod/LSO-LinSpisokObhod.github.io/refs/heads/main/sub/WIFI.txt"),
]

WHITE_SOURCES = [
    ("bikinitw22",  "https://gitverse.ru/api/repos/bikinitw22/apelsintel/raw/branch/main/alive_bs.txt"),
    ("Pizduk",      "https://gitverse.ru/api/repos/Pizduk/PizdukVPN/raw/branch/master/WlSubPiz.txt"),
    ("mos.ru",      "https://hub.mos.ru/kfwl/auto/raw/main/wl"),
    ("RKP-WL",      "https://hub.mos.ru/rkp/sub-roskompozor/raw/main/wl"),
    ("igareck-WL",  "https://raw.githack.com/igareck/vpn-configs-for-russia/main/Vless-Reality-White-Lists-Rus-Mobile.txt"),
    ("etoneya wl",  "https://etoskam.ru/whitelist"),
    ("ring-team",   "https://enc.ring-team.casa/sub/kuajs27ilzcz"),
    ("ImSketch",    "https://raw.githubusercontent.com/ImSketch1337/vless-/refs/heads/main/BLWLservers.txt"),
    ("LSO-LTE",     "https://raw.githubusercontent.com/LSO-LinSpisokObhod/LSO-LinSpisokObhod.github.io/refs/heads/main/sub/LTE.txt"),
    ("Akres-WL",    "https://hub.mos.ru/akres/vpn/-/raw/main/bwl"),
]

COUNTRY_NAMES = {
    "AD":"Andorra","AE":"UAE","AF":"Afghanistan","AG":"Antigua","AL":"Albania","AM":"Armenia",
    "AO":"Angola","AR":"Argentina","AT":"Austria","AU":"Australia","AZ":"Azerbaijan",
    "BA":"Bosnia","BB":"Barbados","BD":"Bangladesh","BE":"Belgium","BF":"Burkina Faso",
    "BG":"Bulgaria","BH":"Bahrain","BI":"Burundi","BJ":"Benin","BN":"Brunei","BO":"Bolivia",
    "BR":"Brazil","BS":"Bahamas","BT":"Bhutan","BW":"Botswana","BY":"Belarus","BZ":"Belize",
    "CA":"Canada","CD":"Congo","CF":"CAR","CG":"Congo","CH":"Switzerland","CI":"Côte d'Ivoire",
    "CL":"Chile","CM":"Cameroon","CN":"China","CO":"Colombia","CR":"Costa Rica","CU":"Cuba",
    "CV":"Cape Verde","CY":"Cyprus","CZ":"Czechia","DE":"Germany","DJ":"Djibouti","DK":"Denmark",
    "DM":"Dominica","DO":"Dominican Rep","DZ":"Algeria","EC":"Ecuador","EE":"Estonia",
    "EG":"Egypt","ER":"Eritrea","ES":"Spain","ET":"Ethiopia","FI":"Finland","FJ":"Fiji",
    "FM":"Micronesia","FR":"France","GA":"Gabon","GB":"United Kingdom","GD":"Grenada",
    "GE":"Georgia","GH":"Ghana","GM":"Gambia","GN":"Guinea","GQ":"Eq. Guinea","GR":"Greece",
    "GT":"Guatemala","GW":"Guinea-Bissau","GY":"Guyana","HK":"Hong Kong","HN":"Honduras",
    "HR":"Croatia","HT":"Haiti","HU":"Hungary","ID":"Indonesia","IE":"Ireland","IL":"Israel",
    "IN":"India","IQ":"Iraq","IR":"Iran","IS":"Iceland","IT":"Italy","JM":"Jamaica",
    "JO":"Jordan","JP":"Japan","KE":"Kenya","KG":"Kyrgyzstan","KH":"Cambodia","KI":"Kiribati",
    "KM":"Comoros","KN":"St Kitts","KP":"North Korea","KR":"South Korea","KW":"Kuwait",
    "KZ":"Kazakhstan","LA":"Laos","LB":"Lebanon","LC":"St Lucia","LI":"Liechtenstein",
    "LK":"Sri Lanka","LR":"Liberia","LS":"Lesotho","LT":"Lithuania","LU":"Luxembourg",
    "LV":"Latvia","LY":"Libya","MA":"Morocco","MC":"Monaco","MD":"Moldova","ME":"Montenegro",
    "MG":"Madagascar","MH":"Marshall Is","MK":"N. Macedonia","ML":"Mali","MM":"Myanmar",
    "MN":"Mongolia","MO":"Macao","MR":"Mauritania","MT":"Malta","MU":"Mauritius",
    "MV":"Maldives","MW":"Malawi","MX":"Mexico","MY":"Malaysia","MZ":"Mozambique",
    "NA":"Namibia","NE":"Niger","NG":"Nigeria","NI":"Nicaragua","NL":"Netherlands",
    "NO":"Norway","NP":"Nepal","NR":"Nauru","NZ":"New Zealand","OM":"Oman","PA":"Panama",
    "PE":"Peru","PG":"Papua N.G.","PH":"Philippines","PK":"Pakistan","PL":"Poland",
    "PT":"Portugal","PW":"Palau","PY":"Paraguay","QA":"Qatar","RO":"Romania","RS":"Serbia",
    "RU":"Russia","RW":"Rwanda","SA":"Saudi Arabia","SB":"Solomon Is","SC":"Seychelles",
    "SD":"Sudan","SE":"Sweden","SG":"Singapore","SI":"Slovenia","SK":"Slovakia",
    "SL":"Sierra Leone","SM":"San Marino","SN":"Senegal","SO":"Somalia","SR":"Suriname",
    "SS":"South Sudan","ST":"São Tomé","SV":"El Salvador","SY":"Syria","SZ":"Eswatini",
    "TD":"Chad","TG":"Togo","TH":"Thailand","TJ":"Tajikistan","TL":"Timor-Leste",
    "TM":"Turkmenistan","TN":"Tunisia","TO":"Tonga","TR":"Turkey","TT":"Trinidad",
    "TV":"Tuvalu","TW":"Taiwan","TZ":"Tanzania","UA":"Ukraine","UG":"Uganda","US":"USA",
    "UY":"Uruguay","UZ":"Uzbekistan","VA":"Vatican","VC":"St Vincent","VE":"Venezuela",
    "VN":"Vietnam","VU":"Vanuatu","WS":"Samoa","YE":"Yemen","ZA":"South Africa",
    "ZM":"Zambia","ZW":"Zimbabwe",
}

XRAY_BIN = os.path.expanduser("~/xray/xray")
TMP_DIR = "/tmp/xray_tmp"
SOCKS_BASE_PORT = 20000
TEST_URL = "https://www.gstatic.com/generate_204"
PING_TIMEOUT = 5
XRAY_WORKERS = 20

BLOCKED_ASNS = {
    "13335", "14789", "132892", "202623", "203898", "209242",
    "395747", "400095", "402542",
    "16509",
    "396982",
    "8075",
    "20860"
}

FINGERPRINTS = ["firefox", "safari", "ios", "android", "edge", "random", "randomized"]

_FLAGS = {}


def random_fp():
    return random.choice(FINGERPRINTS)


def parse_config(line, source_label):
    line = line.strip()
    if not line or line.startswith('#'):
        return None
    try:
        if line.startswith('vmess://'):
            raw = base64.b64decode(line[8:] + '=' * (-len(line[8:]) % 4)).decode('utf-8')
            cfg = json.loads(raw)
            return {'host': cfg['add'], 'port': int(cfg['port']), 'raw': line,
                    'label': source_label, 'type': 'VMess'}
        elif line.startswith('vless://'):
            p = urlparse(line)
            return {'host': p.hostname, 'port': p.port or 443, 'raw': line,
                    'label': source_label, 'type': 'VLESS'}
        elif line.startswith('trojan://'):
            p = urlparse(line)
            return {'host': p.hostname, 'port': p.port or 443, 'raw': line,
                    'label': source_label, 'type': 'Trojan'}
        elif line.startswith('ss://'):
            p = urlparse(line)
            return {'host': p.hostname, 'port': p.port or 8388, 'raw': line,
                    'label': source_label, 'type': 'Shadowsocks'}
        elif line.startswith(('hysteria2://', 'hy2://')):
            p = urlparse(line)
            return {'host': p.hostname, 'port': p.port or 443, 'raw': line,
                    'label': source_label, 'type': 'Hysteria2'}
        elif line.startswith('tuic://'):
            p = urlparse(line)
            return {'host': p.hostname, 'port': p.port or 443, 'raw': line,
                    'label': source_label, 'type': 'TUIC'}
    except Exception:
        return None
    return None


def dedup_configs(configs):
    seen = set()
    unique = []
    for cfg in configs:
        key = cfg['raw'].split('#')[0]
        if key not in seen:
            seen.add(key)
            unique.append(cfg)
    return unique


def check_asn(host):
    try:
        req = urllib.request.Request(f"https://getmyip.pro/api/{host}",
            headers={'User-Agent': 'Mozilla/5.0'})
        resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
        asn = resp.get("asn")
        if asn:
            return str(asn).replace("AS", "")
    except Exception:
        pass

    try:
        req = urllib.request.Request(f"https://ipdata.info/{host}",
            headers={'User-Agent': 'Mozilla/5.0'})
        resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
        asn = resp.get("asn")
        if asn:
            return str(asn).replace("AS", "")
    except Exception:
        pass

    try:
        req = urllib.request.Request(f"http://ip-api.com/json/{host}?fields=as",
            headers={'User-Agent': 'Mozilla/5.0'})
        resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
        as_str = resp.get("as", "")
        if as_str:
            return as_str.split()[0].replace("AS", "")
    except Exception:
        pass

    return ""


def fetch_sub(url, retries=1):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*', 'Accept-Language': 'ru,en;q=0.9', 'Referer': url,
            })
            resp = urllib.request.urlopen(req, timeout=20)
            data = resp.read()
            if not data:
                return [], "Пустой ответ", "empty"
            try:
                decoded = base64.b64decode(data + b'=' * (-len(data) % 4)).decode('utf-8', errors='ignore')
                if any(p in decoded for p in ('vless://', 'vmess://', 'trojan://', 'ss://', 'hysteria2://')):
                    return decoded.splitlines(), None, None
            except Exception:
                pass
            text = data.decode('utf-8', errors='ignore')
            if any(p in text for p in ('vless://', 'vmess://', 'trojan://', 'ss://', 'hysteria2://')):
                return text.splitlines(), None, None
            head = text[:500].lower()
            if '<html' in head or '<!doctype' in head or '<body' in head:
                return [], "HTML-страница вместо подписки", "html"
            if 'proxies:' in head or 'proxy-groups:' in head:
                return [], "YAML (Clash) — не поддерживается", "yaml"
            if 'access denied' in head or 'forbidden' in head or '403' in head:
                return [], "Доступ запрещён (403)", "forbidden"
            if 'not found' in head or '404' in head:
                return [], "Не найдено (404)", "notfound"
            if len(text.strip()) < 20:
                return [], f"Слишком короткий ответ ({len(text)} байт)", "short"
            b64_blocks = re.findall(r'[A-Za-z0-9+/=]{100,}', text)
            for block in b64_blocks:
                try:
                    dec = base64.b64decode(block + '=' * (-len(block) % 4)).decode('utf-8', errors='ignore')
                    if any(p in dec for p in ('vless://', 'vmess://', 'trojan://', 'ss://')):
                        return dec.splitlines(), None, None
                except Exception:
                    continue
            return [], f"Не распознан формат ({len(text)} байт)", "unknown"
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(5)
                continue
            return [], f"HTTP {e.code} {e.reason}", f"http{e.code}"
        except urllib.error.URLError as e:
            if attempt < retries and 'timed out' in str(e).lower():
                time.sleep(3)
                continue
            return [], f"Сеть: {e.reason}", "network"
        except Exception as e:
            return [], f"{type(e).__name__}: {e}", "error"
    return [], "Все попытки провалились", "failed"


def load_flags_batch(hosts):
    ips = [h for h in hosts if h and h.count('.') == 3 and all(p.isdigit() for p in h.split('.'))]
    if not ips:
        return
    print(f"\n🌍 Определение стран для {len(ips)} IP...")
    for i in range(0, len(ips), 100):
        chunk = ips[i:i+100]
        try:
            payload = json.dumps([{"query": ip} for ip in chunk]).encode()
            req = urllib.request.Request("http://ip-api.com/batch?fields=countryCode,query",
                data=payload, headers={'Content-Type': 'application/json'})
            resp = json.loads(urllib.request.urlopen(req, timeout=10).read())
            for item in resp:
                _FLAGS[item['query']] = item.get('countryCode', '')
        except Exception as e:
            print(f"   ⚠️ Батч {i//100}: {e}")
        time.sleep(1.5)


def get_flag_and_name(host):
    code = _FLAGS.get(host, '')
    if not code or len(code) != 2:
        return "🌐", "Unknown"
    return chr(ord(code[0]) + 127397) + chr(ord(code[1]) + 127397), COUNTRY_NAMES.get(code, code)


def format_line(cfg, ping, idx=0):
    flag, country = get_flag_and_name(cfg['host'])
    new_name = f"cool [{cfg['label']}] {flag} |#{idx}|"
    raw = cfg['raw']
    if '#' in raw:
        base, _ = raw.rsplit('#', 1)
        return f"{base}#{quote(new_name)}"
    return f"{raw}#{quote(new_name)}"


def add_metadata(lines, title, update_hours=4, support_url=None, announce=None):
    meta = []
    meta.append(f"#profile-title: {title}")
    meta.append(f"#profile-update-interval: {update_hours}")
    if support_url:
        meta.append(f"#support-url: {support_url}")
    if announce:
        meta.append(f"#announce: {announce}")
    meta.append("")
    return meta + lines


def to_clash_proxy(cfg, flag, country, idx):
    uri = cfg['raw'].strip()
    name = f"cool [{cfg['label']}] {flag} |#{idx}|"
    try:
        if uri.startswith('vless://'):
            p = urlparse(uri)
            q = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
            proxy = {
                "name": name, "type": "vless", "server": p.hostname,
                "port": p.port or 443, "uuid": p.username, "udp": True,
                "tls": q.get("security") in ("tls", "reality"),
                "network": q.get("type", "tcp"),
                "skip-cert-verify": q.get("allowInsecure", "0") == "1",
            }
            if q.get("flow"):
                proxy["flow"] = q["flow"]
            if q.get("security") == "reality":
                proxy["reality-opts"] = {"public-key": q.get("pbk", ""), "short-id": q.get("sid", "")}
                proxy["servername"] = q.get("sni", "")
                proxy["client-fingerprint"] = random_fp()
            elif q.get("security") == "tls":
                proxy["servername"] = q.get("sni", p.hostname)
            if q.get("type") == "ws":
                proxy["ws-opts"] = {"path": q.get("path", "/"), "headers": {"Host": q.get("host", "")}}
            elif q.get("type") == "grpc":
                proxy["grpc-opts"] = {"grpc-service-name": q.get("serviceName", "")}
            return proxy

        elif uri.startswith('vmess://'):
            raw = base64.b64decode(uri[8:] + '=' * (-len(uri[8:]) % 4)).decode('utf-8')
            v = json.loads(raw)
            proxy = {
                "name": name, "type": "vmess", "server": v.get("add"),
                "port": int(v.get("port", 443)), "uuid": v.get("id"),
                "alterId": int(v.get("aid", 0)), "cipher": v.get("scy", "auto"),
                "udp": True, "tls": v.get("tls") == "tls",
                "network": v.get("net", "tcp"),
            }
            if v.get("sni"):
                proxy["servername"] = v["sni"]
            if v.get("net") == "ws":
                proxy["ws-opts"] = {"path": v.get("path", "/"), "headers": {"Host": v.get("host", "")}}
            return proxy

        elif uri.startswith('trojan://'):
            p = urlparse(uri)
            q = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
            return {
                "name": name, "type": "trojan", "server": p.hostname,
                "port": p.port or 443, "password": p.username,
                "sni": q.get("sni", p.hostname), "udp": True,
                "skip-cert-verify": False,
            }

        elif uri.startswith('ss://'):
            raw = uri[5:]
            if "#" in raw:
                raw = raw.split("#")[0]
            if "@" in raw:
                userinfo, hostport = raw.rsplit("@", 1)
                try:
                    userinfo = base64.b64decode(userinfo + '=' * (-len(userinfo) % 4)).decode()
                except Exception:
                    pass
            else:
                decoded = base64.b64decode(raw + '=' * (-len(raw) % 4)).decode()
                userinfo, hostport = decoded.rsplit("@", 1)
            method, password = userinfo.split(":", 1)
            host, port = hostport.split(":")
            return {
                "name": name, "type": "ss", "server": host,
                "port": int(port.split("/")[0]), "cipher": method, "password": password, "udp": True,
            }

        elif uri.startswith(('hysteria2://', 'hy2://')):
            p = urlparse(uri)
            q = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
            return {
                "name": name, "type": "hysteria2", "server": p.hostname,
                "port": p.port or 443, "password": p.username,
                "sni": q.get("sni", p.hostname),
                "skip-cert-verify": q.get("insecure", "0") == "1",
            }
    except Exception:
        return None
    return None


def build_clash_yaml(working_list, out_path):
    proxies = []
    skipped = 0
    seen_names = {}
    country_map = {}
    non_ru_names = []

    for idx, (cfg, ping) in enumerate(working_list, 1):
        flag, country = get_flag_and_name(cfg['host'])
        p = to_clash_proxy(cfg, flag, country, idx)
        if not p:
            skipped += 1
            continue
        base_name = p["name"]
        if base_name in seen_names:
            seen_names[base_name] += 1
            p["name"] = f"{base_name} #{seen_names[base_name]}"
        else:
            seen_names[base_name] = 1
        proxies.append(p)
        country_map.setdefault(country, []).append(p["name"])
        if country != "Russia":
            non_ru_names.append(p["name"])

    if not proxies:
        print(f"⚠️ {out_path}: нет прокси")
        return

    all_names = [p["name"] for p in proxies]
    code_by_name = {v: k for k, v in COUNTRY_NAMES.items()}

    groups = [
        {"name": "Обычный", "type": "select",
         "proxies": ["Авто", "Авто (без RU)"] + all_names},
        {"name": "Авто", "type": "url-test",
         "url": "http://www.gstatic.com/generate_204",
         "interval": 300, "tolerance": 50,
         "proxies": all_names},
        {"name": "Авто (без RU)", "type": "url-test",
         "url": "http://www.gstatic.com/generate_204",
         "interval": 300, "tolerance": 50,
         "proxies": non_ru_names if non_ru_names else all_names},
    ]

    for country in sorted(country_map.keys()):
        names = country_map[country]
        if country == "Unknown":
            group_name = "🌐 Unknown"
        else:
            code = code_by_name.get(country)
            if code:
                flag = chr(ord(code[0]) + 127397) + chr(ord(code[1]) + 127397)
                group_name = f"{flag} {country}"
            else:
                group_name = country
        groups.append({
            "name": group_name,
            "type": "url-test",
            "url": "http://www.gstatic.com/generate_204",
            "interval": 300, "tolerance": 50,
            "proxies": names,
        })

    data = {
        "proxies": proxies,
        "proxy-groups": groups,
        "rules": ["MATCH,Обычный"],
    }

    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, width=1000)
    dupes = sum(v - 1 for v in seen_names.values() if v > 1)
    print(f"💾 {out_path}: {len(proxies)} прокси, {len(country_map)} стран, "
          f"без RU {len(non_ru_names)} (пропущено {skipped}, дубликатов {dupes})")


def save_stats(working_list, out_path):
    countries = {}
    for cfg, _ in working_list:
        _, country = get_flag_and_name(cfg['host'])
        countries[country] = countries.get(country, 0) + 1
    top_countries = sorted(countries.items(), key=lambda x: -x[1])[:10]
    stats = {
        "total": len(working_list),
        "countries": len(countries),
        "top": [{"country": c, "count": n} for c, n in top_countries],
        "updated": int(time.time()),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"💾 {out_path}: {stats['total']} серверов, {stats['countries']} стран")


def xray_vless(uri):
    p = urlparse(uri)
    q = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
    cfg = {
        "protocol": "vless",
        "settings": {
            "vnext": [{
                "address": p.hostname,
                "port": p.port or 443,
                "users": [{
                    "id": p.username,
                    "encryption": q.get("encryption", "none"),
                    "flow": q.get("flow", "")
                }]
            }]
        },
        "streamSettings": {
            "network": q.get("type", "tcp"),
            "security": q.get("security", "none")
        }
    }
    if q.get("security") == "reality":
        cfg["streamSettings"]["realitySettings"] = {
            "serverName": q.get("sni", ""),
            "publicKey": q.get("pbk", ""),
            "shortId": q.get("sid", ""),
            "fingerprint": random_fp()
        }
    if q.get("security") == "tls":
        cfg["streamSettings"]["tlsSettings"] = {
            "serverName": q.get("sni", p.hostname),
            "allowInsecure": q.get("allowInsecure", "0") == "1"
        }
    return cfg


def xray_vmess(uri):
    raw = base64.b64decode(uri[8:] + '=' * (-len(uri[8:]) % 4)).decode()
    v = json.loads(raw)
    ss = {
        "network": v.get("net", "tcp"),
        "security": v.get("tls", "") or "none"
    }
    if v.get("tls") == "tls":
        ss["tlsSettings"] = {"serverName": v.get("sni") or v.get("host", "")}
    if v.get("net") == "ws":
        ss["wsSettings"] = {
            "path": v.get("path", "/"),
            "headers": {"Host": v.get("host", "")}
        }
    return {
        "protocol": "vmess",
        "settings": {
            "vnext": [{
                "address": v.get("add"),
                "port": int(v.get("port", 443)),
                "users": [{
                    "id": v.get("id"),
                    "alterId": int(v.get("aid", 0)),
                    "security": v.get("scy", "auto")
                }]
            }]
        },
        "streamSettings": ss
    }


def xray_trojan(uri):
    p = urlparse(uri)
    q = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
    return {
        "protocol": "trojan",
        "settings": {
            "servers": [{
                "address": p.hostname,
                "port": p.port or 443,
                "password": p.username
            }]
        },
        "streamSettings": {
            "network": q.get("type", "tcp"),
            "security": "tls",
            "tlsSettings": {"serverName": q.get("sni", p.hostname)}
        }
    }


def xray_ss(uri):
    raw = uri[5:]
    if "#" in raw:
        raw = raw.split("#")[0]
    if "@" in raw:
        userinfo, hostport = raw.rsplit("@", 1)
        try:
            userinfo = base64.b64decode(userinfo + '=' * (-len(userinfo) % 4)).decode()
        except Exception:
            pass
    else:
        decoded = base64.b64decode(raw + '=' * (-len(raw) % 4)).decode()
        userinfo, hostport = decoded.rsplit("@", 1)
    method, password = userinfo.split(":", 1)
    host, port = hostport.split(":")
    return {
        "protocol": "shadowsocks",
        "settings": {
            "servers": [{
                "address": host,
                "port": int(port.split("/")[0]),
                "method": method,
                "password": password
            }]
        }
    }


def xray_hysteria2(uri):
    p = urlparse(uri)
    q = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
    return {
        "protocol": "hysteria2",
        "settings": {
            "servers": [{
                "address": p.hostname,
                "port": p.port or 443,
                "password": p.username
            }]
        },
        "streamSettings": {
            "security": "tls",
            "tlsSettings": {"serverName": q.get("sni", p.hostname)}
        }
    }


def xray_config(uri):
    uri = uri.strip()
    try:
        if uri.startswith("vless://"):
            return xray_vless(uri)
        if uri.startswith("vmess://"):
            return xray_vmess(uri)
        if uri.startswith("trojan://"):
            return xray_trojan(uri)
        if uri.startswith("ss://"):
            return xray_ss(uri)
        if uri.startswith(("hysteria2://", "hy2://")):
            return xray_hysteria2(uri)
    except Exception:
        return None
    return None


def check_via_xray(args):
    idx, cfg = args
    outbound = xray_config(cfg['raw'])
    if not outbound:
        return None, None
    port = SOCKS_BASE_PORT + (idx % 500)
    cfg_path = os.path.join(TMP_DIR, f"c{idx}.json")
    full = {
        "log": {"loglevel": "none"},
        "inbounds": [{
            "listen": "127.0.0.1",
            "port": port,
            "protocol": "socks",
            "settings": {"auth": "noauth", "udp": False}
        }],
        "outbounds": [outbound]
    }
    try:
        with open(cfg_path, "w") as f:
            json.dump(full, f)
    except Exception:
        return None, None
    proc = subprocess.Popen([XRAY_BIN, "-c", cfg_path],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ready = False
    for _ in range(30):
        time.sleep(0.1)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                ready = True
                break
        except Exception:
            continue
    if not ready:
        proc.kill()
        try:
            os.remove(cfg_path)
        except Exception:
            pass
        return None, None
    try:
        start = time.time()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({
            "http": f"socks5://127.0.0.1:{port}",
            "https": f"socks5://127.0.0.1:{port}"
        }))
        req = urllib.request.Request(TEST_URL, headers={"User-Agent": "Mozilla/5.0"})
        resp = opener.open(req, timeout=PING_TIMEOUT)
        ping = int((time.time() - start) * 1000)
        proc.kill()
        try:
            os.remove(cfg_path)
        except Exception:
            pass
        return (cfg, ping) if resp.status == 204 else (None, None)
    except Exception:
        proc.kill()
        try:
            os.remove(cfg_path)
        except Exception:
            pass
        return None, None


def run_subscription(sources, remote_name, label):
    global _FLAGS
    _FLAGS = {}
    t0 = time.time()

    print(f"\n═══ {label} → {remote_name} ═══")
    print(f"⚙️  Xray-потоков: {XRAY_WORKERS} | Пинг-таймаут: {PING_TIMEOUT}s | URL: {TEST_URL}")

    all_configs = []
    type_counter = Counter()
    for src_label, url in sources:
        lines, error, reason = fetch_sub(url, retries=1)
        got = 0
        for line in lines:
            cfg = parse_config(line, src_label)
            if cfg:
                all_configs.append(cfg)
                type_counter[cfg['type']] += 1
                got += 1
        if error:
            print(f"   ❌ [{src_label}] {error}")
        elif got == 0 and reason:
            print(f"   ⚠️  [{src_label}] {reason}")
        else:
            print(f"   ✅ [{src_label}] {len(lines)} строк → {got} конфигов")

    print(f"\n✅ Всего конфигов: {len(all_configs)}")
    if not all_configs:
        print("⚠️ Нечего проверять")
        return

    before = len(all_configs)
    all_configs = dedup_configs(all_configs)
    after = len(all_configs)
    if before > after:
        print(f"♻️  Дедупликация: {before} → {after} (убрано {before - after})")

    load_flags_batch(list({c['host'] for c in all_configs}))

    print(f"\n🔥 Xray-проверка (GET {TEST_URL}) в {XRAY_WORKERS} потоков...")
    print("   ⏳ Это медленно — может занять 15-40 минут...")

    working = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=XRAY_WORKERS) as ex:
        tasks = list(enumerate(all_configs))
        for i, (cfg, ping) in enumerate(ex.map(check_via_xray, tasks)):
            if cfg and ping is not None:
                working.append((cfg, ping))
            if (i + 1) % 20 == 0:
                elapsed = time.time() - t0
                speed = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (len(all_configs) - i - 1) / speed if speed > 0 else 0
                print(f"   {i+1}/{len(all_configs)} | рабочих: {len(working)} | {speed:.1f}/с | ETA {eta:.0f}с")

    print(f"\n🔥 Рабочих через прокси: {len(working)}")
    if not working:
        print("⚠️ Ничего не прошло проверку")
        return

    print(f"\n🛡️  Фильтр по ASN (ТСПУ-блокировки)...")
    filtered = []
    asn_skipped = 0
    asn_cache = {}
    for cfg, ping in working:
        host = cfg['host']
        if host not in asn_cache:
            asn_cache[host] = check_asn(host)
        asn = asn_cache[host]
        if asn in BLOCKED_ASNS:
            asn_skipped += 1
            continue
        filtered.append((cfg, ping))
    print(f"🚫 Убрано {asn_skipped} конфигов (Cloudflare/Amazon/Google/Microsoft)")
    print(f"✅ Осталось: {len(filtered)}")
    working = filtered

    if not working:
        print("⚠️ После фильтра ASN ничего не осталось")
        return

    random.shuffle(working)
    output = [format_line(c, p, i + 1) for i, (c, p) in enumerate(working)]

    if "black" in remote_name.lower():
        title = "CoolSubs — Black"
    else:
        title = "CoolSubs — White"

    announce = "могут быть нерабочие сервера под видом рабочих"

    output = add_metadata(
        output,
        title,
        update_hours=4,
        support_url="https://github.com/vessel-web/CoolSubs",
        announce=announce,
    )

    with open(remote_name, "w", encoding="utf-8") as f:
        f.write("\n".join(output))
    print(f"💾 {remote_name}: {len(output)} серверов (TXT)")

    yaml_name = remote_name.replace(".txt", ".yaml")
    build_clash_yaml(working, yaml_name)

    stats_name = remote_name.replace(".txt", "_stats.json")
    save_stats(working, stats_name)

    print(f"⏱️  Всего: {time.time() - t0:.1f}с")


if __name__ == "__main__":
    os.makedirs(TMP_DIR, exist_ok=True)
    print("🚀 Запуск парсера с Xray-проверкой...")
    run_subscription(BLACK_SOURCES, "Cool-Black.txt", "⚫ ЧЁРНАЯ")
    run_subscription(WHITE_SOURCES, "Cool-White.txt", "⚪ БЕЛАЯ")
    print("\n✅ Готово!")
