#!/usr/bin/env python3
import urllib.request, urllib.error, base64, json, socket, os, re, gzip, time, random, glob, asyncio, ipaddress
import subprocess, requests, yaml, aiohttp
from urllib.parse import urlparse, quote, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import socks
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False

BLACK_SOURCES = [
    ("Gidroksi","https://gidroksi.fun/black-list"),
    ("Aetris","https://gitverse.ru/api/repos/flaafix/AetrisVPN_Black_list/raw/branch/master/configs.txt"),
    ("etoneya","https://etoskam.ru/other"),
    ("igareck","https://raw.githack.com/igareck/vpn-configs-for-russia/main/BLACK_VLESS_RUS_mobile.txt"),
    ("Diversan","https://raw.githubusercontent.com/Diversan313/apex-parser/main/subs/main/alive_bl.txt"),
    ("luxxuria","https://github.com/luxxuria/harvester/raw/refs/heads/main/non_ru.txt"),
    ("Akres","https://github.com/theakres/VPN/raw/main/all"),
    ("VLESSFORU","https://sub.vlessfo.ru/vlessforu/working_configs.txt"),
    ("RKP","https://hub.mos.ru/rkp/sub-roskompozor/raw/main/bl"),
    ("Pizduk-sub","https://gitverse.ru/api/repos/Pizduk/PizdukVPN/raw/branch/master/sub.txt"),
    ("LimeVPN","https://raw.githubusercontent.com/LimeHi/LimeVPN/refs/heads/main/blacklist.txt"),
    ("atbPars","https://raw.githubusercontent.com/djsigggmagg-ui/atbPars-sub/refs/heads/main/subscription.txt"),
    ("WarpGen","https://warp-gen.cyb-portal.org/CP-039"),
    ("BUNKER","https://hub.mos.ru/akelladejavu/bunker/-/raw/main/BLACK_LIST.txt"),
    ("ImSketch","https://raw.githubusercontent.com/ImSketch1337/vless-/refs/heads/main/BLWLservers.txt"),
    ("LSO-WIFI","https://raw.githubusercontent.com/LSO-LinSpisokObhod/LSO-LinSpisokObhod.github.io/refs/heads/main/sub/WIFI.txt"),
    ("IR-1","https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/v2ray/all_sub.txt"),
    ("IR-2","https://raw.githubusercontent.com/mohammadaz2/v2rayConfigsForYou/main/configs.txt"),
    ("IR-3","https://raw.githubusercontent.com/DukeMehdi/FreeList-V2ray-Configs/main/configs.txt"),
    ("IR-4","https://raw.githubusercontent.com/MahanKenway/Freedom-V2Ray/main/Sub.txt"),
    ("IR-5","https://raw.githubusercontent.com/patterniha/Free-Configs/main/configs.txt"),
    ("IR-6","https://raw.githubusercontent.com/balochscript/free-vpn-configs/gh-pages/subscription-recent.txt"),
    ("dwnl","https://raw.githubusercontent.com/vessel-web/CoolSubs/refs/heads/main/configs.txt.gz"),
]

WHITE_SOURCES = [
    ("bikinitw22","https://gitverse.ru/api/repos/bikinitw22/apelsintel/raw/branch/main/alive_bs.txt"),
    ("Pizduk","https://gitverse.ru/api/repos/Pizduk/PizdukVPN/raw/branch/master/WlSubPiz.txt"),
    ("mos.ru","https://hub.mos.ru/kfwl/auto/raw/main/wl"),
    ("RKP-WL","https://hub.mos.ru/rkp/sub-roskompozor/raw/main/wl"),
    ("igareck-WL","https://raw.githack.com/igareck/vpn-configs-for-russia/main/Vless-Reality-White-Lists-Rus-Mobile.txt"),
    ("etoneya wl","https://etoskam.ru/whitelist"),
    ("ring-team", os.environ.get("RING_TEAM_SUB", "")),
    ("ImSketch","https://raw.githubusercontent.com/ImSketch1337/vless-/refs/heads/main/BLWLservers.txt"),
    ("LSO-LTE","https://raw.githubusercontent.com/LSO-LinSpisokObhod/LSO-LinSpisokObhod.github.io/refs/heads/main/sub/LTE.txt"),
    ("Akres-WL","https://github.com/theakres/VPN/raw/main/bwl"),
    ("BUNKER-WL","https://hub.mos.ru/akelladejavu/bunker/-/raw/main/WHITE_LIST.txt"),
    ("IR-WL-1","https://raw.githubusercontent.com/liketolivefree/kobabi/main/sub.txt"),
    ("IR-WL-2","https://raw.githubusercontent.com/DukeMehdi/FreeList-V2ray-Configs/refs/heads/main/Configs/All-DukeMehdi-Configs.txt"),
    ("IR-WL-3","https://raw.githubusercontent.com/r3zarahimi/tg-v2ray-configs-every2h/main/Config_jo.txt"),
]

COUNTRY_NAMES = {
    "AD":"Andorra","AE":"UAE","AF":"Afghanistan","AG":"Antigua","AL":"Albania","AM":"Armenia",
    "AO":"Angola","AR":"Argentina","AT":"Austria","AU":"Australia","AZ":"Azerbaijan",
    "BA":"Bosnia","BB":"Barbados","BD":"Bangladesh","BE":"Belgium","BF":"Burkina Faso",
    "BG":"Bulgaria","BH":"Bahrain","BI":"Burundi","BJ":"Benin","BN":"Brunei","BO":"Bolivia",
    "BR":"Brazil","BS":"Bahamas","BT":"Bhutan","BW":"Botswana","BY":"Belarus","BZ":"Belize",
    "CA":"Canada","CD":"DR Congo","CF":"CAR","CG":"Congo","CH":"Switzerland","CI":"Côte d'Ivoire",
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

SINGBOX_BIN = os.path.expanduser("~/singbox/sing-box-extended")
TMP_DIR = "/tmp/singbox_tmp"
API_BASE_PORT = 19090
SOCKS_BASE_PORT = 20000
TEST_URL = "https://api.ipify.org?format=json"
PING_TIMEOUT = 5
DELAY_TIMEOUT_MS = 6000
API_START_TIMEOUT = 8.0
KERNEL_COUNT = 4
BATCH_SIZE_PER_CORE = 50
XRAY_WORKERS = 20
RETRY_PAUSE_SEC = 20
MAX_RESPONSE_BYTES = 128 * 1024 * 1024
BLOCKED_ASNS = {"13335","14789","132892","202623","203898","209242","395747","400095","402542","16509","396982","8075","20860"}
FINGERPRINTS = ["firefox","safari","ios","android","edge","random","randomized"]
UTLS_FP = {"chrome","firefox","safari","ios","android","edge","360","qq","random","randomized"}
VMESS_SECURITY = {"auto","none","zero","aes-128-gcm","chacha20-poly1305"}
CLASH_VMESS_CIPHERS = {"auto","none","aes-128-gcm","chacha20-poly1305"}
CLASH_NETWORKS = {"tcp","ws","grpc","http","h2","httpupgrade"}
TUIC_CC = {"cubic","new_reno","bbr"}
TUIC_URM = {"native","quic"}
SS_PLUGINS = {"obfs-local":"obfs-local","simple-obfs":"obfs-local","v2ray-plugin":"v2ray-plugin"}

def _find_geoip_db():
    here = os.path.dirname(os.path.abspath(__file__))
    for root in [here, os.path.join(here, ".."), os.getcwd()]:
        for p in sorted(glob.glob(os.path.join(root, "GeoLite2-Country*.mmdb"))):
            try:
                if os.path.getsize(p) > 1_000_000:
                    return p
            except Exception:
                pass
    return None

def _find_dbip_db():
    here = os.path.dirname(os.path.abspath(__file__))
    for root in [here, os.path.join(here, ".."), os.getcwd()]:
        for p in sorted(glob.glob(os.path.join(root, "dbip-asn-lite*.mmdb"))):
            try:
                if os.path.getsize(p) > 100_000:
                    return p
            except Exception:
                pass
    return None

GEOIP_DB = _find_geoip_db()
DBIP_DB = _find_dbip_db()
_FLAGS = {}
_ASN = {}
_STATS_BL = {}
_STATS_WL = {}
_RESOLVE_CACHE = {}
SB_GEN_ERRORS = {}
SB_RUN_ERRORS = {}

def _b64(s):
    s = s.strip()
    return urlsafe_b64decode_padded(s)

def urlsafe_b64decode_padded(s):
    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))

def random_fp():
    return random.choice(FINGERPRINTS)

def sample_ratio(configs, ratio=0.3):
    if not configs:
        return configs
    return random.sample(configs, max(1, int(len(configs) * ratio)))

def parse_config(line, source_label):
    line = line.strip()
    if not line or line.startswith('#'):
        return None
    try:
        if line.startswith('vmess://'):
            raw = _b64(line[8:]).decode('utf-8')
            cfg = json.loads(raw)
            return {'host': cfg['add'], 'port': int(cfg['port']), 'raw': line, 'label': source_label, 'type': 'VMess'}
        if line.startswith('vless://'):
            p = urlparse(line); return {'host': p.hostname, 'port': p.port or 443, 'raw': line, 'label': source_label, 'type': 'VLESS'}
        if line.startswith('trojan://'):
            p = urlparse(line); return {'host': p.hostname, 'port': p.port or 443, 'raw': line, 'label': source_label, 'type': 'Trojan'}
        if line.startswith('ss://'):
            p = urlparse(line); return {'host': p.hostname, 'port': p.port or 8388, 'raw': line, 'label': source_label, 'type': 'Shadowsocks'}
        if line.startswith(('hysteria2://', 'hy2://')):
            p = urlparse(line); return {'host': p.hostname, 'port': p.port or 443, 'raw': line, 'label': source_label, 'type': 'Hysteria2'}
        if line.startswith('tuic://'):
            p = urlparse(line); return {'host': p.hostname, 'port': p.port or 443, 'raw': line, 'label': source_label, 'type': 'TUIC'}
    except Exception:
        return None
    return None

def dedup_configs(configs):
    seen, unique = set(), []
    for cfg in configs:
        k = cfg['raw'].split('#')[0]
        if k not in seen:
            seen.add(k); unique.append(cfg)
    return unique

def save_unsorted(configs, out_path):
    seen, lines = set(), []
    for cfg in configs:
        raw = cfg['raw'].split('#')[0]
        if raw in seen:
            continue
        seen.add(raw); lines.append(cfg['raw'])
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"💾 {out_path}: {len(lines)} сырых конфигов")

def resolve_host(host):
    if not host:
        return None
    if host in _RESOLVE_CACHE:
        return _RESOLVE_CACHE[host]
    try:
        ipaddress.ip_address(host)
        _RESOLVE_CACHE[host] = host
        return host
    except ValueError:
        pass
    ip = None
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        v4 = [i[4][0] for i in infos if i[0] == socket.AF_INET]
        v6 = [i[4][0] for i in infos if i[0] == socket.AF_INET6]
        ip = (v4 or v6 or [None])[0]
    except Exception:
        ip = None
    _RESOLVE_CACHE[host] = ip
    return ip

def load_asn_batch(hosts):
    global _ASN
    _ASN = {}
    unique_hosts = list({h for h in hosts if h})
    if not unique_hosts:
        return
    host_to_ip = {}
    with ThreadPoolExecutor(max_workers=50) as ex:
        for h, ip in zip(unique_hosts, ex.map(resolve_host, unique_hosts)):
            if ip:
                host_to_ip[h] = ip
    print(f"🌐 ASN: {len(host_to_ip)}/{len(unique_hosts)} хостов резолвнуто")
    if not DBIP_DB:
        print("⚠️ dbip-asn-lite*.mmdb не найден")
        return
    try:
        import geoip2.database
        reader = geoip2.database.Reader(DBIP_DB)
        found = 0
        for h, ip in host_to_ip.items():
            try:
                r = reader.asn(ip)
                if r.autonomous_system_number:
                    _ASN[h] = str(r.autonomous_system_number)
                    found += 1
            except Exception:
                pass
        reader.close()
        print(f"✅ DBIP ASN: {found}/{len(host_to_ip)} ({DBIP_DB})")
    except Exception as e:
        print(f"⚠️ DBIP ASN: {e}")

def fetch_sub(url, retries=1):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*', 'Accept-Language': 'ru,en;q=0.9', 'Referer': url,
            })
            resp = urllib.request.urlopen(req, timeout=20)
            data = resp.read(MAX_RESPONSE_BYTES + 1)
            if len(data) > MAX_RESPONSE_BYTES:
                return [], "Ответ слишком большой", "toolarge"
            if url.endswith(".gz"):
                try:
                    data = gzip.decompress(data)
                except Exception:
                    pass
            if not data:
                return [], "Пустой ответ", "empty"
            try:
                decoded = base64.b64decode(data + b'=' * (-len(data) % 4)).decode('utf-8', errors='ignore')
                if any(p in decoded for p in ('vless://', 'vmess://', 'trojan://', 'ss://', 'hysteria2://', 'hy2://', 'tuic://')):
                    return decoded.splitlines(), None, None
            except Exception:
                pass
            text = data.decode('utf-8', errors='ignore')
            if any(p in text for p in ('vless://', 'vmess://', 'trojan://', 'ss://', 'hysteria2://', 'hy2://', 'tuic://')):
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
            for block in re.findall(r'[A-Za-z0-9+/=]{100,}', text):
                try:
                    dec = base64.b64decode(block + '=' * (-len(block) % 4)).decode('utf-8', errors='ignore')
                    if any(p in dec for p in ('vless://', 'vmess://', 'trojan://', 'ss://')):
                        return dec.splitlines(), None, None
                except Exception:
                    continue
            return [], f"Не распознан формат ({len(text)} байт)", "unknown"
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(5); continue
            return [], f"HTTP {e.code} {e.reason}", f"http{e.code}"
        except urllib.error.URLError as e:
            if attempt < retries and 'timed out' in str(e).lower():
                time.sleep(3); continue
            return [], f"Сеть: {e.reason}", "network"
        except Exception as e:
            return [], f"{type(e).__name__}: {e}", "error"
    return [], "Все попытки провалились", "failed"

def load_flags_batch(hosts):
    global _FLAGS
    _FLAGS = {}
    if not GEOIP_DB:
        print("⚠️ GeoLite2-Country*.mmdb не найден — все будут 🌐")
        return
    try:
        import geoip2.database
        reader = geoip2.database.Reader(GEOIP_DB)
    except Exception as e:
        print(f"⚠️ GeoLite2 не загружен: {e} — все будут 🌐")
        return
    print(f"🌍 GeoLite2: {GEOIP_DB} ({os.path.getsize(GEOIP_DB)//1024} КБ)")
    unique_hosts = list({h for h in hosts if h})
    host_to_ip = {}
    with ThreadPoolExecutor(max_workers=50) as ex:
        for host, ip in zip(unique_hosts, ex.map(resolve_host, unique_hosts)):
            if ip:
                host_to_ip[host] = ip
    print(f"   ✅ Резолвнуто: {len(host_to_ip)}/{len(unique_hosts)}")
    found = 0
    for host, ip in host_to_ip.items():
        try:
            resp = reader.country(ip)
            if resp.country.iso_code:
                _FLAGS[host] = resp.country.iso_code; found += 1
        except Exception:
            pass
    reader.close()
    print(f"✅ Определено стран: {found}/{len(unique_hosts)}")

def get_flag_and_name(host):
    code = _FLAGS.get(host, '')
    if not code or len(code) != 2:
        return "🌐", "Unknown"
    return chr(ord(code[0]) + 127397) + chr(ord(code[1]) + 127397), COUNTRY_NAMES.get(code, code)

def format_line(cfg, idx=0):
    flag, _ = get_flag_and_name(cfg['host'])
    new_name = f"cool [{cfg['label']}] {flag} |#{idx}|"
    raw = cfg['raw']
    if '#' in raw:
        base, _ = raw.rsplit('#', 1); return f"{base}#{quote(new_name)}"
    return f"{raw}#{quote(new_name)}"

def add_metadata(lines, title, update_hours=4, support_url=None, announce=None):
    meta = [f"#profile-title: {title}", f"#profile-update-interval: {update_hours}"]
    if support_url:
        meta.append(f"#support-url: {support_url}")
    if announce:
        meta.append(f"#announce: {announce}")
    meta.append("")
    return meta + lines

def _q(uri):
    p = urlparse(uri)
    return p, {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}

def sb_vless(uri):
    p, q = _q(uri)
    if not p.hostname:
        return None
    if q.get("allowInsecure") == "1":
        return None
    uuid = unquote(p.username or "")
    if not uuid:
        return None
    security = q.get("security") or ("reality" if q.get("pbk") else "")
    out = {"type": "vless", "server": p.hostname, "server_port": p.port or 443, "uuid": uuid}
    if security == "reality":
        pbk = q.get("pbk", "")
        if not pbk:
            return None
        out["tls"] = {"enabled": True, "server_name": q.get("sni") or p.hostname,
                      "reality": {"enabled": True, "public_key": pbk, "short_id": q.get("sid", "")},
                      "utls": {"enabled": True, "fingerprint": q.get("fp") if q.get("fp") in UTLS_FP else random_fp()}}
    elif security == "tls":
        out["tls"] = {"enabled": True, "server_name": q.get("sni") or p.hostname}
        if q.get("alpn"):
            out["tls"]["alpn"] = [a for a in q["alpn"].split(",") if a]
        if q.get("fp") in UTLS_FP:
            out["tls"]["utls"] = {"enabled": True, "fingerprint": q["fp"]}
    net = q.get("type", "tcp")
    if net == "ws":
        tr = {"type": "ws", "path": q.get("path") or "/"}
        if q.get("host"):
            tr["headers"] = {"Host": q["host"]}
        out["transport"] = tr
    elif net == "grpc":
        out["transport"] = {"type": "grpc", "service_name": q.get("serviceName", "")}
    elif net == "httpupgrade":
        out["transport"] = {"type": "httpupgrade", "host": q.get("host") or p.hostname,
                            "path": q.get("path") or "/"}
    elif net in ("xhttp", "splithttp"):
        out["transport"] = {"type": "xhttp", "host": q.get("host") or p.hostname,
                            "path": q.get("path") or "/", "mode": q.get("mode", "auto")}
    elif net in ("h2", "h2c", "http"):
        out["transport"] = {"type": "http", "host": [q.get("host") or p.hostname],
                            "path": q.get("path") or "/"}
    flow = q.get("flow", "")
    if flow == "xtls-rprx-vision" and "transport" not in out and out.get("tls"):
        out["flow"] = flow
    return out

def sb_vmess(uri):
    raw = _b64(uri[8:]).decode("utf-8", errors="ignore")
    v = json.loads(raw)
    if not v.get("add") or not v.get("id"):
        return None
    if v.get("allowInsecure") in (True, "1", "true"):
        return None
    try:
        port = int(v.get("port") or 443)
    except Exception:
        return None
    scy = (v.get("scy") or "auto").strip().lower()
    if scy not in VMESS_SECURITY:
        return None
    try:
        aid = int(v.get("aid") or 0)
    except Exception:
        aid = 0
    out = {"type": "vmess", "server": v["add"], "server_port": port,
           "uuid": v["id"], "security": scy, "alter_id": aid}
    if v.get("tls") in ("tls", "reality"):
        out["tls"] = {"enabled": True, "server_name": v.get("sni") or v.get("host") or v["add"]}
        if v.get("alpn"):
            out["tls"]["alpn"] = [a for a in str(v["alpn"]).split(",") if a]
        if v.get("fp") in UTLS_FP:
            out["tls"]["utls"] = {"enabled": True, "fingerprint": v["fp"]}
    net = (v.get("net") or "tcp").lower()
    if net == "ws":
        tr = {"type": "ws", "path": v.get("path") or "/"}
        if v.get("host"):
            tr["headers"] = {"Host": v["host"]}
        out["transport"] = tr
    elif net == "grpc":
        out["transport"] = {"type": "grpc", "service_name": v.get("path") or ""}
    elif net == "httpupgrade":
        out["transport"] = {"type": "httpupgrade", "host": v.get("host") or v["add"],
                            "path": v.get("path") or "/"}
    elif net in ("h2", "h2c", "http") or (net == "tcp" and v.get("type") == "http"):
        out["transport"] = {"type": "http", "host": [v.get("host") or v["add"]],
                            "path": v.get("path") or "/"}
    return out

def sb_trojan(uri):
    p, q = _q(uri)
    if not p.hostname:
        return None
    if q.get("allowInsecure") == "1":
        return None
    password = unquote(p.username or "")
    if not password:
        return None
    tls_cfg = {"enabled": True, "server_name": q.get("sni") or q.get("peer") or p.hostname}
    if q.get("alpn"):
        tls_cfg["alpn"] = [a for a in q["alpn"].split(",") if a]
    if q.get("fp") in UTLS_FP:
        tls_cfg["utls"] = {"enabled": True, "fingerprint": q["fp"]}
    out = {"type": "trojan", "server": p.hostname, "server_port": p.port or 443,
           "password": password, "tls": tls_cfg}
    net = q.get("type", "tcp")
    if net == "ws":
        tr = {"type": "ws", "path": q.get("path") or "/"}
        if q.get("host"):
            tr["headers"] = {"Host": q["host"]}
        out["transport"] = tr
    elif net == "grpc":
        out["transport"] = {"type": "grpc", "service_name": q.get("serviceName", "")}
    elif net == "httpupgrade":
        out["transport"] = {"type": "httpupgrade", "host": q.get("host") or p.hostname,
                            "path": q.get("path") or "/"}
    return out

def sb_ss(uri):
    u = uri[5:].split("#", 1)[0]
    if "?" in u:
        base, qs = u.split("?", 1)
        q = {k: v[0] for k, v in parse_qs(qs, keep_blank_values=True).items()}
    else:
        base, q = u, {}
    if "@" in base:
        userinfo, hostport = base.rsplit("@", 1)
        userinfo = unquote(userinfo)
        if ":" not in userinfo:
            try:
                userinfo = base64.b64decode(userinfo + '=' * (-len(userinfo) % 4)).decode("utf-8", errors="ignore")
            except Exception:
                return None
    else:
        try:
            decoded = base64.b64decode(base + '=' * (-len(base) % 4)).decode("utf-8", errors="ignore")
        except Exception:
            return None
        if "@" not in decoded:
            return None
        userinfo, hostport = decoded.rsplit("@", 1)
    if ":" not in userinfo:
        return None
    method, password = userinfo.split(":", 1)
    method = method.strip().lower()
    if not method:
        return None
    hp = urlparse("//" + hostport)
    if not hp.hostname:
        return None
    try:
        port = hp.port or 8388
    except ValueError:
        return None
    out = {"type": "shadowsocks", "server": hp.hostname, "server_port": port,
           "method": method, "password": unquote(password)}
    plugin = q.get("plugin", "")
    if plugin:
        parts = plugin.split(";", 1)
        pname = SS_PLUGINS.get(parts[0].strip())
        if not pname:
            return None
        out["plugin"] = pname
        if len(parts) > 1 and parts[1]:
            out["plugin_opts"] = parts[1]
    return out

def sb_hysteria2(uri):
    p, q = _q(uri)
    if not p.hostname:
        return None
    if q.get("insecure", q.get("allowInsecure", "0")) == "1":
        return None
    password = unquote(p.username or "") or q.get("auth", "")
    if not password:
        return None
    tls_cfg = {"enabled": True, "server_name": q.get("sni") or q.get("peer") or p.hostname}
    if q.get("alpn"):
        tls_cfg["alpn"] = [a for a in q["alpn"].split(",") if a]
    out = {"type": "hysteria2", "server": p.hostname, "server_port": p.port or 443,
           "password": password, "tls": tls_cfg}
    if q.get("obfs") == "salamander":
        out["obfs"] = {"type": "salamander", "password": q.get("obfs-password", "")}
    return out

def sb_tuic(uri):
    p, q = _q(uri)
    if not p.hostname:
        return None
    if q.get("allow_insecure", q.get("allowInsecure", q.get("insecure", "0"))) == "1":
        return None
    uid = unquote(p.username or "")
    if not uid:
        return None
    pwd = unquote(p.password or "")
    cc = q.get("congestion_control", "cubic")
    if cc not in TUIC_CC:
        cc = "cubic"
    urm = q.get("udp_relay_mode", "native")
    if urm not in TUIC_URM:
        urm = "native"
    tls_cfg = {"enabled": True, "server_name": q.get("sni") or q.get("peer") or p.hostname}
    alpn = q.get("alpn")
    tls_cfg["alpn"] = [a for a in alpn.split(",") if a] if alpn else ["h3"]
    return {"type": "tuic", "server": p.hostname, "server_port": p.port or 443,
            "uuid": uid, "password": pwd, "congestion_control": cc, "udp_relay_mode": urm,
            "tls": tls_cfg}

def sb_config(uri):
    uri = uri.strip()
    try:
        if uri.startswith("vless://"): return sb_vless(uri)
        if uri.startswith("vmess://"): return sb_vmess(uri)
        if uri.startswith("trojan://"): return sb_trojan(uri)
        if uri.startswith("ss://"): return sb_ss(uri)
        if uri.startswith(("hysteria2://", "hy2://")): return sb_hysteria2(uri)
        if uri.startswith("tuic://"): return sb_tuic(uri)
    except Exception:
        return None
    return None

def _note_err(store, key):
    store[key] = store.get(key, 0) + 1

def _next_api_port(worker_id):
    return API_BASE_PORT + worker_id * 100

def _next_socks_port(worker_id):
    return SOCKS_BASE_PORT + worker_id * 100 + 100

def _probe_kernel():
    probe_cfg = os.path.join(TMP_DIR, "probe.json")
    probe_port = API_BASE_PORT + 5000
    socks_port = SOCKS_BASE_PORT + 5000
    cfg = {
        "log": {"level": "error"},
        "experimental": {"clash_api": {"external_controller": f"127.0.0.1:{probe_port}"}},
        "inbounds": [{"type": "socks", "listen": "127.0.0.1", "listen_port": socks_port}],
        "outbounds": [{"type": "direct", "tag": "direct"}],
        "route": {"rules": [{"action": "route", "outbound": "direct"}]},
    }
    proc = None
    try:
        with open(probe_cfg, "w") as f:
            json.dump(cfg, f)
        proc = subprocess.Popen([SINGBOX_BIN, "run", "-c", probe_cfg],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + API_START_TIMEOUT
        ok = False
        while time.time() < deadline:
            try:
                r = requests.get(f"http://127.0.0.1:{probe_port}/version", timeout=0.5)
                if r.status_code == 200:
                    ok = True; break
            except Exception:
                pass
            time.sleep(0.1)
        print(f"{'✅' if ok else '❌'} probe: API :{probe_port} {'поднялся' if ok else 'не поднялся'}")
        return ok
    except Exception as e:
        print(f"❌ probe: {type(e).__name__}: {e}")
        return False
    finally:
        if proc is not None:
            try:
                proc.kill(); proc.wait(timeout=3)
            except Exception:
                pass
        try: os.remove(probe_cfg)
        except Exception: pass

async def _wait_api_async(session, port, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            async with session.get(f"http://127.0.0.1:{port}/version",
                                    timeout=aiohttp.ClientTimeout(total=0.5)) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        await asyncio.sleep(0.05)
    return False

async def _delay_one_async(session, api_port, tag):
    try:
        url = f"http://127.0.0.1:{api_port}/proxies/{tag}/delay"
        timeout = aiohttp.ClientTimeout(total=DELAY_TIMEOUT_MS / 1000.0 + 2)
        async with session.get(url,
                                params={"url": TEST_URL, "timeout": DELAY_TIMEOUT_MS},
                                timeout=timeout) as r:
            if r.status == 200:
                data = await r.json()
                d = data.get("delay")
                if isinstance(d, int) and d > 0:
                    return tag, d
            return tag, None
    except Exception:
        return tag, None

async def _run_batch_async(batch_cfgs, worker_id, session):
    api_port = _next_api_port(worker_id)
    socks_port = _next_socks_port(worker_id)
    cfg_path = os.path.join(TMP_DIR, f"w{worker_id}.json")
    err_path = os.path.join(TMP_DIR, f"w{worker_id}.err")
    outbounds, tag_to_cfg = [], {}
    for j, cfg in enumerate(batch_cfgs):
        ob = sb_config(cfg["raw"])
        if not ob:
            scheme = cfg["raw"].split("://")[0] if "://" in cfg["raw"] else "unknown"
            _note_err(SB_GEN_ERRORS, scheme)
            continue
        tag = f"p{j}"
        ob["tag"] = tag
        outbounds.append(ob)
        tag_to_cfg[tag] = cfg
    if not outbounds:
        return []
    full = {
        "log": {"level": "error"},
        "experimental": {"clash_api": {"external_controller": f"127.0.0.1:{api_port}"}},
        "inbounds": [{"type": "socks", "listen": "127.0.0.1", "listen_port": socks_port}],
        "outbounds": outbounds,
        "route": {"rules": [{"action": "route", "outbound": ob["tag"]} for ob in outbounds]},
    }
    try:
        with open(cfg_path, "w") as f:
            json.dump(full, f)
    except Exception:
        return []
    proc, errf = None, None
    try:
        errf = open(err_path, "wb")
        proc = await asyncio.create_subprocess_exec(
            SINGBOX_BIN, "run", "-c", cfg_path,
            stdout=asyncio.subprocess.DEVNULL, stderr=errf,
        )
        if not await _wait_api_async(session, api_port, API_START_TIMEOUT):
            _note_err(SB_RUN_ERRORS, "api_not_ready")
            proc.kill()
            try: await proc.wait()
            except Exception: pass
            proc = None
            return await _split_retry(batch_cfgs, worker_id, session)
        results1 = await asyncio.gather(*[_delay_one_async(session, api_port, t) for t in tag_to_cfg])
        first_pass = {t: d for t, d in results1 if d is not None and t in tag_to_cfg}
        if not first_pass:
            return []
        await asyncio.sleep(RETRY_PAUSE_SEC)
        results2 = await asyncio.gather(*[_delay_one_async(session, api_port, t) for t in first_pass.keys()])
        return [(tag_to_cfg[t], d) for t, d in results2 if d is not None and t in first_pass]
    except Exception as e:
        _note_err(SB_RUN_ERRORS, f"{type(e).__name__}")
        return []
    finally:
        if proc is not None:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        if errf is not None:
            try: errf.close()
            except Exception: pass
        for pth in (cfg_path, err_path):
            try: os.remove(pth)
            except Exception: pass

async def _split_retry(batch_cfgs, worker_id, session):
    if len(batch_cfgs) <= 1:
        return []
    mid = len(batch_cfgs) // 2
    left = await _run_batch_async(batch_cfgs[:mid], worker_id, session)
    right = await _run_batch_async(batch_cfgs[mid:], worker_id, session)
    return left + right

async def _batch_worker(worker_id, queue, session, results, lock, stats, total):
    while True:
        try:
            _, batch = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        res = await _run_batch_async(batch, worker_id, session)
        async with lock:
            results.extend(res)
            stats["done"] += len(batch)
            elapsed = time.time() - stats["t0"]
            speed = stats["done"] / elapsed if elapsed > 0 else 0
            eta = (total - stats["done"]) / speed if speed > 0 else 0
            print(f"   [w{worker_id}] {stats['done']}/{total} | рабочих {len(results)} | {speed:.1f}/с | ETA {eta:.0f}с")

async def _check_batch_main(all_configs):
    queue = asyncio.Queue()
    for i in range(0, len(all_configs), BATCH_SIZE_PER_CORE):
        queue.put_nowait((i // BATCH_SIZE_PER_CORE, all_configs[i:i+BATCH_SIZE_PER_CORE]))
    results = []
    lock = asyncio.Lock()
    stats = {"done": 0, "t0": time.time()}
    connector = aiohttp.TCPConnector(limit=BATCH_SIZE_PER_CORE * KERNEL_COUNT * 4)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [_batch_worker(i, queue, session, results, lock, stats, len(all_configs))
                   for i in range(KERNEL_COUNT)]
        await asyncio.gather(*workers)
    return results

def _check_one_legacy(idx, cfg):
    ob = sb_config(cfg["raw"])
    if not ob:
        return None
    port = SOCKS_BASE_PORT + (idx % 500) + 1000
    cfg_path = os.path.join(TMP_DIR, f"fb{idx}.json")
    err_path = os.path.join(TMP_DIR, f"fb{idx}.err")
    full = {
        "log": {"level": "error"},
        "inbounds": [{"type": "socks", "listen": "127.0.0.1", "listen_port": port}],
        "outbounds": [ob],
    }
    proc = None
    errf = None
    try:
        with open(cfg_path, "w") as f:
            json.dump(full, f)
        errf = open(err_path, "wb")
        proc = subprocess.Popen([SINGBOX_BIN, "run", "-c", cfg_path],
                                stdout=subprocess.DEVNULL, stderr=errf)
        ready = False
        for _ in range(30):
            time.sleep(0.1)
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                    ready = True; break
            except Exception:
                if proc.poll() is not None:
                    break
        if not ready:
            return None
        start = time.time()
        resp = requests.get(TEST_URL, timeout=PING_TIMEOUT,
                            proxies={"http": f"socks5h://127.0.0.1:{port}",
                                     "https": f"socks5h://127.0.0.1:{port}"},
                            headers={"User-Agent": "Mozilla/5.0"})
        ping = int((time.time() - start) * 1000)
        if resp.status_code == 200 and "ip" in resp.text:
            return (cfg, ping)
        return None
    except Exception:
        return None
    finally:
        if proc is not None:
            try:
                proc.kill(); proc.wait(timeout=3)
            except Exception:
                pass
        if errf is not None:
            try: errf.close()
            except Exception: pass
        for pth in (cfg_path, err_path):
            try: os.remove(pth)
            except Exception: pass

def _fallback_check(all_configs):
    if not HAS_SOCKS:
        print("❌ PySocks не установлен — fallback невозможен")
        return []
    print(f"   fallback: legacy ({XRAY_WORKERS} потоков)")
    results = []
    with ThreadPoolExecutor(max_workers=XRAY_WORKERS) as ex:
        futures = {ex.submit(_check_one_legacy, i, c): c for i, c in enumerate(all_configs)}
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            if r:
                results.append(r)
            if i % 50 == 0:
                print(f"   fallback {i}/{len(all_configs)} | рабочих {len(results)}")
    return results

def check_via_singbox_batch(all_configs):
    if not all_configs:
        return []
    if not _probe_kernel():
        print("❌ Ядро недоступно — fallback")
        return _fallback_check(all_configs)
    print(f"🔥 batch: {KERNEL_COUNT} ядра × {BATCH_SIZE_PER_CORE} = {KERNEL_COUNT * BATCH_SIZE_PER_CORE} | timeout {DELAY_TIMEOUT_MS}ms | URL {TEST_URL} | double-pass")
    return asyncio.run(_check_batch_main(all_configs))

def to_clash_proxy(cfg, flag, country, idx):
    uri = cfg['raw'].strip()
    name = f"cool [{cfg['label']}] {flag} |#{idx}|"
    try:
        if uri.startswith('vless://'):
            p = urlparse(uri)
            q = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
            security = q.get("security") or ("reality" if q.get("pbk") else "")
            net = q.get("type", "tcp") or "tcp"
            if net not in CLASH_NETWORKS:
                return None
            uuid = unquote(p.username or "")
            if not uuid or not p.hostname:
                return None
            proxy = {"name": name, "type": "vless", "server": p.hostname, "port": p.port or 443,
                     "uuid": uuid, "udp": True, "tls": security in ("tls", "reality"),
                     "network": net, "skip-cert-verify": q.get("allowInsecure", "0") == "1"}
            if q.get("flow") == "xtls-rprx-vision" and net == "tcp":
                proxy["flow"] = q["flow"]
            if security == "reality":
                if not q.get("pbk"):
                    return None
                proxy["reality-opts"] = {"public-key": q.get("pbk", ""), "short-id": q.get("sid", "")}
                proxy["servername"] = q.get("sni", "") or p.hostname
                proxy["client-fingerprint"] = random_fp()
            elif security == "tls":
                proxy["servername"] = q.get("sni", "") or p.hostname
            if net == "ws":
                proxy["ws-opts"] = {"path": q.get("path", "/")}
                if q.get("host"):
                    proxy["ws-opts"]["headers"] = {"Host": q["host"]}
            elif net == "grpc":
                proxy["grpc-opts"] = {"grpc-service-name": q.get("serviceName", "")}
            elif net == "httpupgrade":
                proxy["httpupgrade-opts"] = {"path": q.get("path", "/"), "host": q.get("host", "") or p.hostname}
            return proxy
        if uri.startswith('vmess://'):
            raw = _b64(uri[8:]).decode('utf-8', errors='ignore')
            v = json.loads(raw)
            cipher = (v.get("scy") or "auto").strip().lower()
            if cipher not in CLASH_VMESS_CIPHERS:
                return None
            net = (v.get("net") or "tcp").lower()
            if net == "tcp" and v.get("type") == "http":
                net = "http"
            if net not in CLASH_NETWORKS:
                return None
            try:
                aid = int(v.get("aid") or 0)
            except Exception:
                aid = 0
            try:
                vmport = int(v.get("port") or 443)
            except Exception:
                return None
            if vmport <= 0 or not v.get("add") or not v.get("id"):
                return None
            proxy = {"name": name, "type": "vmess", "server": v["add"], "port": vmport,
                     "uuid": v["id"], "alterId": aid, "cipher": cipher,
                     "udp": True, "tls": v.get("tls") == "tls", "network": net}
            if v.get("sni") or v.get("host"):
                proxy["servername"] = v.get("sni") or v.get("host")
            if net == "ws":
                proxy["ws-opts"] = {"path": v.get("path", "/")}
                if v.get("host"):
                    proxy["ws-opts"]["headers"] = {"Host": v["host"]}
            elif net == "grpc":
                proxy["grpc-opts"] = {"grpc-service-name": v.get("path", "")}
            elif net == "http":
                proxy["http-opts"] = {"path": [v.get("path", "/")], "headers": {"Host": [v.get("host") or v.get("add")]}}
            return proxy
        if uri.startswith('trojan://'):
            p = urlparse(uri)
            q = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
            if not p.username or not p.hostname:
                return None
            net = q.get("type", "tcp") or "tcp"
            proxy = {"name": name, "type": "trojan", "server": p.hostname, "port": p.port or 443,
                     "password": unquote(p.username), "sni": q.get("sni", "") or p.hostname,
                     "udp": True, "skip-cert-verify": q.get("allowInsecure", "0") == "1"}
            if net == "ws":
                proxy["network"] = "ws"
                proxy["ws-opts"] = {"path": q.get("path", "/")}
                if q.get("host"):
                    proxy["ws-opts"]["headers"] = {"Host": q["host"]}
            elif net == "grpc":
                proxy["network"] = "grpc"
                proxy["grpc-opts"] = {"grpc-service-name": q.get("serviceName", "")}
            elif net != "tcp":
                return None
            return proxy
        if uri.startswith('ss://'):
            u = uri[5:].split("#", 1)[0]
            if "?" in u:
                base, qs = u.split("?", 1)
                q = {k: v[0] for k, v in parse_qs(qs, keep_blank_values=True).items()}
            else:
                base, q = u, {}
            if "@" in base:
                userinfo, hostport = base.rsplit("@", 1)
                userinfo = unquote(userinfo)
                if ":" not in userinfo:
                    try: userinfo = base64.b64decode(userinfo + '=' * (-len(userinfo) % 4)).decode()
                    except Exception: return None
            else:
                try:
                    decoded = base64.b64decode(base + '=' * (-len(base) % 4)).decode()
                except Exception:
                    return None
                if "@" not in decoded:
                    return None
                userinfo, hostport = decoded.rsplit("@", 1)
            if ":" not in userinfo:
                return None
            method, password = userinfo.split(":", 1)
            hp = urlparse("//" + hostport)
            if not hp.hostname:
                return None
            proxy = {"name": name, "type": "ss", "server": hp.hostname, "port": hp.port or 8388,
                     "cipher": method.strip().lower(), "password": unquote(password), "udp": True}
            plugin = q.get("plugin", "")
            if plugin:
                parts = plugin.split(";", 1)
                popts = parts[1] if len(parts) > 1 else ""
                if parts[0].strip() in ("obfs-local", "simple-obfs"):
                    opts = dict(x.split("=", 1) for x in popts.split(";") if "=" in x)
                    proxy["plugin"] = "obfs"
                    proxy["plugin-opts"] = {"mode": opts.get("obfs", "http")}
                    if opts.get("obfs-host"):
                        proxy["plugin-opts"]["host"] = opts["obfs-host"]
                else:
                    return None
            return proxy
        if uri.startswith(('hysteria2://', 'hy2://')):
            p = urlparse(uri)
            q = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
            password = unquote(p.username or "") or q.get("auth", "")
            if not password or not p.hostname:
                return None
            proxy = {"name": name, "type": "hysteria2", "server": p.hostname, "port": p.port or 443,
                     "password": password, "sni": q.get("sni", "") or p.hostname,
                     "skip-cert-verify": q.get("insecure", q.get("allowInsecure", "0")) == "1"}
            if q.get("obfs") == "salamander":
                proxy["obfs"] = "salamander"
                proxy["obfs-password"] = q.get("obfs-password", "")
            return proxy
    except Exception:
        return None
    return None

def build_clash_yaml(working_list, out_path):
    proxies, skipped, country_map, non_ru = [], 0, {}, []
    for idx, (cfg, _) in enumerate(working_list, 1):
        flag, country = get_flag_and_name(cfg['host'])
        p = to_clash_proxy(cfg, flag, country, idx)
        if not p:
            skipped += 1; continue
        proxies.append(p)
        country_map.setdefault(country, []).append(p["name"])
        if country != "Russia":
            non_ru.append(p["name"])
    if not proxies:
        print(f"⚠️ {out_path}: нет прокси"); return
    all_names = [p["name"] for p in proxies]
    code_by_name = {v: k for k, v in COUNTRY_NAMES.items()}
    groups = [
        {"name": "Обычный", "type": "select", "proxies": ["Авто", "Авто (без RU)"] + all_names},
        {"name": "Авто", "type": "url-test", "url": "http://www.gstatic.com/generate_204",
         "interval": 300, "tolerance": 50, "proxies": all_names},
        {"name": "Авто (без RU)", "type": "url-test", "url": "http://www.gstatic.com/generate_204",
         "interval": 300, "tolerance": 50, "proxies": non_ru if non_ru else all_names},
    ]
    for country in sorted(country_map):
        names = country_map[country]
        if country == "Unknown":
            gname = "🌐 Unknown"
        else:
            code = code_by_name.get(country)
            gname = (chr(ord(code[0]) + 127397) + chr(ord(code[1]) + 127397) + " " + country) if code else country
        groups.append({"name": gname, "type": "url-test", "url": "http://www.gstatic.com/generate_204",
                       "interval": 300, "tolerance": 50, "proxies": names})
    data = {"proxies": proxies, "proxy-groups": groups, "rules": ["MATCH,Обычный"]}
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, width=1000)
    print(f"💾 {out_path}: {len(proxies)} прокси, {len(country_map)} стран, без RU {len(non_ru)} (пропущено {skipped})")

def save_stats(working_list, out_path):
    countries = {}
    for cfg, _ in working_list:
        _, country = get_flag_and_name(cfg['host'])
        countries[country] = countries.get(country, 0) + 1
    top = sorted(countries.items(), key=lambda x: -x[1])[:10]
    stats = {"total": len(working_list), "countries": len(countries),
             "top": [{"country": c, "count": n} for c, n in top], "updated": int(time.time())}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"💾 {out_path}: {stats['total']} серверов, {stats['countries']} стран")

def update_changelog(stats_bl, stats_wl, changelog_path="CHANGELOG.md"):
    try:
        old = open(changelog_path, "r", encoding="utf-8").read()
    except Exception:
        old = "# Changelog\n\nИстория обновлений CoolSubs.\n\n"
    prev_bl, prev_wl = 0, 0
    for line in old.split("\n"):
        if "BL:" in line and "WL:" in line:
            try:
                parts = line.split("BL:")[1].split("WL:")
                prev_bl = int(parts[0].strip().split()[0]); prev_wl = int(parts[1].strip().split()[0])
            except Exception:
                pass
            break
    now = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    bl, wl = stats_bl["total"], stats_wl["total"]
    dbl = (bl - prev_bl) if prev_bl else 0
    dwl = (wl - prev_wl) if prev_wl else 0
    line = f"- **{now}** — BL: {bl} ({'+' if dbl>0 else ''}{dbl}), WL: {wl} ({'+' if dwl>0 else ''}{dwl})"
    if old.startswith("# Changelog"):
        he = old.find("\n\n")
        new = (old + "\n" + line + "\n") if he == -1 else (old[:he+2] + line + "\n" + old[he+2:].rstrip() + "\n")
    else:
        new = "# Changelog\n\n" + line + "\n"
    lines = new.split("\n")
    if len(lines) > 200:
        new = "\n".join(lines[:200])
    with open(changelog_path, "w", encoding="utf-8") as f:
        f.write(new)
    print(f"💾 {changelog_path}: BL {bl}, WL {wl}")

def run_subscription(sources, remote_name, label):
    global _FLAGS, _ASN, _STATS_BL, _STATS_WL, SB_GEN_ERRORS, SB_RUN_ERRORS
    _FLAGS = {}
    _ASN = {}
    SB_GEN_ERRORS, SB_RUN_ERRORS = {}, {}
    t0 = time.time()
    print(f"\n═══ {label} → {remote_name} ═══")
    all_configs = []
    for src_label, url in sources:
        if not url:
            continue
        if src_label == "dwnl" and (os.path.exists("configs.txt") or os.path.exists("configs.txt.gz")):
            print(f"   ✅ [dwnl] локальный configs.txt")
            try:
                if os.path.exists("configs.txt"):
                    lines = open("configs.txt", "r", encoding="utf-8", errors="ignore").read().splitlines()
                else:
                    with gzip.open("configs.txt.gz", "rt", encoding="utf-8", errors="ignore") as f:
                        lines = f.read().splitlines()
                error, reason = None, None
            except Exception as e:
                print(f"   ❌ [dwnl] ошибка чтения: {e}"); continue
        else:
            lines, error, reason = fetch_sub(url, retries=1)
        src_configs = [c for c in (parse_config(l, src_label) for l in lines) if c]
        if src_label.startswith("IR-"):
            before = len(src_configs); src_configs = sample_ratio(src_configs, 0.3)
            print(f"   ✅ [{src_label}] {len(lines)} строк → {before} конфигов → взято {len(src_configs)} (30%)")
        elif error:
            print(f"   ❌ [{src_label}] {error}")
        elif not src_configs and reason:
            print(f"   ⚠️  [{src_label}] {reason}")
        else:
            print(f"   ✅ [{src_label}] {len(lines)} строк → {len(src_configs)} конфигов")
        all_configs.extend(src_configs)
    print(f"\n✅ Всего конфигов: {len(all_configs)}")
    if not all_configs:
        print("⚠️ Нечего проверять"); return
    save_unsorted(all_configs, "unsorted-black.txt" if "black" in remote_name.lower() else "unsorted-white.txt")
    before = len(all_configs); all_configs = dedup_configs(all_configs)
    if before > len(all_configs):
        print(f"♻️  Дедупликация: {before} → {len(all_configs)} (убрано {before - len(all_configs)})")
    load_flags_batch(list({c['host'] for c in all_configs}))
    print(f"\n🔥 batch-проверка через Clash API...")
    working = check_via_singbox_batch(all_configs)
    print(f"\n🔥 Рабочих: {len(working)}")
    if SB_GEN_ERRORS:
        print("🧹 Не построен outbound: " + ", ".join(f"{k} ×{v}" for k, v in sorted(SB_GEN_ERRORS.items(), key=lambda kv: -kv[1])[:8]))
    if SB_RUN_ERRORS:
        for msg, n in sorted(SB_RUN_ERRORS.items(), key=lambda kv: -kv[1])[:5]:
            print(f"   run [{n}x]: {msg}")
    if not working:
        print("⚠️ Ничего не прошло проверку"); return
    print(f"\n🛡️  Фильтр по ASN...")
    load_asn_batch(list({c['host'] for c, _ in working}))
    filtered, asn_skipped = [], 0
    for cfg, ping in working:
        if _ASN.get(cfg['host'], "") in BLOCKED_ASNS:
            asn_skipped += 1
            continue
        filtered.append((cfg, ping))
    print(f"🚫 Убрано {asn_skipped} (Cloudflare/Amazon/Google/Microsoft)")
    print(f"✅ Осталось: {len(filtered)}")
    working = filtered
    if not working:
        print("⚠️ После фильтра ASN ничего не осталось"); return
    random.shuffle(working)
    output = [format_line(c, i + 1) for i, (c, _) in enumerate(working)]
    title = "CoolSubs — Black" if "black" in remote_name.lower() else "CoolSubs — White"
    output = add_metadata(output, title, update_hours=4,
                          support_url="https://github.com/vessel-web/CoolSubs",
                          announce="могут быть нерабочие сервера под видом рабочих")
    with open(remote_name, "w", encoding="utf-8") as f:
        f.write("\n".join(output))
    print(f"💾 {remote_name}: {len(output)} серверов (TXT)")
    build_clash_yaml(working, remote_name.replace(".txt", ".yaml"))
    save_stats(working, remote_name.replace(".txt", "_stats.json"))
    if "black" in remote_name.lower():
        _STATS_BL = {"total": len(working)}
    else:
        _STATS_WL = {"total": len(working)}
    print(f"⏱️  Всего: {time.time() - t0:.1f}с")

if __name__ == "__main__":
    os.makedirs(TMP_DIR, exist_ok=True)
    print("🚀 sing-box-extended async batch parser (anti-phantom)")
    if not os.path.exists(SINGBOX_BIN):
        print(f"❌ Бинарник не найден: {SINGBOX_BIN}"); raise SystemExit(1)
    print(f"🌍 GeoLite2: {GEOIP_DB}")
    print(f"🌐 DBIP ASN: {DBIP_DB}")
    run_subscription(BLACK_SOURCES, "Cool-Black.txt", "⚫ ЧЁРНАЯ")
    run_subscription(WHITE_SOURCES, "Cool-White.txt", "⚪ БЕЛАЯ")
    if _STATS_BL and _STATS_WL:
        update_changelog(_STATS_BL, _STATS_WL)
    print("\n✅ Готово!")