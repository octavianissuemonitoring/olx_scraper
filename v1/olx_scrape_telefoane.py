"""
OLX – Scraper (date hardcodate): GeoNode endpoints + OLX login + retry backoff + export CSV/XLSX/JSONL
------------------------------------------------------------------------------------------------------
Colectează per anunț:
  telefon, titlu, pret, pret_valoare, pret_moneda, persoana, garantie, descriere, id_anunt, vizualizari, vanzator, url

Funcționalități:
- Login OLX (autologin cu email/parolă; fallback logare asistată).
- Rotație între endpoint-uri GeoNode (round-robin). (La tine e un singur endpoint rotating – suficient.)
- Retry cu backoff exponențial (schimbă endpoint + user-agent la fiecare încercare).
- “Stealth”: UA pool, limbă ro-RO, viewport random, ascundere navigator.webdriver.
- Export CSV (UTF-8 BOM), XLSX, și JSONL (LLM-friendly).

IMPORTANT:
- Respectă Termenii OLX; nu folosi datele pentru spam.
- Fișierul conține credențiale – păstrează-l privat.
"""

# =========================
# 🔧 COMPLETEAZĂ AICI (DATELE TALE)
# =========================

# --- OLX login ---
OLX_EMAIL    = "octavian_rusu@yahoo.com"   # << scrie aici
OLX_PASSWORD = "Mostenire39!"            # << scrie aici

# --- GeoNode endpoints (lista ROTATĂ) ---
# Ai furnizat un endpoint rotating identic pe toate liniile -> e suficient 1 item.
ENDPOINTS = [
    {
        "protocol": "http",
        "host": "proxy.geonode.io",
        "port": 9000,
        "username": "geonode_3UlswT3blD-type-residential",
        "password": "41a8aed7-f884-4940-b016-af7c82c684a8",
    }
]

# Dacă ai IP whitelisting și NU folosești user/parolă, lasă "" pentru username/password.
# Dacă vei adăuga și alte endpoint-uri, copiază dictul de mai sus și schimbă host/port/username/password.

# Opțional: dacă vezi erori SSL de la proxy, pune False temporar.
VERIFY_SSL = True

# =========================
# (NU E NEVOIE SĂ MODIFICI MAI JOS)
# =========================

import csv, json, random, re, time, unicodedata
from dataclasses import dataclass
from itertools import cycle
from typing import Dict, List, Tuple, Optional

import pandas as pd
from seleniumwire import webdriver as wire_webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# --------- CONFIG GENERAL ----------
SEARCH_URL = "https://www.olx.ro/oferte/q-inchiriere-autorulota/"
ROTATE_PER = "page"        # "page" sau "ad"
MAX_PAGE_RETRIES = 3
MAX_AD_RETRIES   = 3
BACKOFF_BASE_SEC = 1.0
BACKOFF_FACTOR   = 2.0
BACKOFF_JITTER   = 0.35
HEADLESS = False
VIEWPORT_W = (1200, 1920)
VIEWPORT_H = (740, 1080)
SLEEP_NAV_MIN = 0.25
SLEEP_NAV_MAX = 0.6
SLEEP_BETWEEN_ADS = 0.8
MAX_EMPTY_PAGES_BEFORE_STOP = 2
REQUIRE_LOGIN = True
ASSISTED_LOGIN_TIMEOUT = 90
COOKIES_SHARE_BETWEEN_DRIVERS = True
EXPORT_JSONL = True
OUTPUT_PREFIX = "anunturi_autorulote"

# --------- UA POOL ----------
UA_POOL = [
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "Win32"},
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "Win32"},
    {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "MacIntel"},
]

# --------- REGEX ----------
PHONE_RE = re.compile(
    r"(?:\+?4?0\s*7[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
    r"|(?:07[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
)
RE_ID = re.compile(r"\bID(?:-ul)?(?:\s*anuntului)?\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
RE_VIEWS = re.compile(r"\bVizualizari\s*[:#]?\s*([\d\s\.]+)", re.IGNORECASE)
RE_GARANTIE = re.compile(r"\bGarantie\b.*?[:\-]?\s*([\d\s\.]+(?:\s*(?:RON|Lei|EUR|€))?)", re.IGNORECASE)

# --------- UTILS ----------
def jitter_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

def exp_backoff_sleep(attempt: int) -> None:
    base = BACKOFF_BASE_SEC * (BACKOFF_FACTOR ** (attempt - 1))
    jitter = base * BACKOFF_JITTER
    time.sleep(random.uniform(base - jitter, base + jitter))

def clean_phone(s: str) -> str:
    digits = re.sub(r"\D", "", s)
    if digits.startswith("40") and len(digits) == 11 and digits[2] == "7":
        digits = "0" + digits[2:]
    if digits.startswith("7") and len(digits) == 9:
        digits = "0" + digits
    return digits

def strip_diacritics(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

def text_or_empty(el) -> str:
    try: return el.text.strip()
    except Exception: return ""

def first_text_by_selectors(driver, selectors: List[Tuple[str, str]]) -> str:
    for by, sel in selectors:
        try:
            el = driver.find_element(by, sel)
            t = text_or_empty(el)
            if t: return t
        except Exception:
            continue
    return ""

def sanitize_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_price(raw: str):
    if not raw: return "", ""
    m = re.search(r"([\d\.\s]+)\s*([€EeUuRrOo]|RON|Lei|LEI)?", raw)
    if not m: return "", ""
    val = re.sub(r"[^\d]", "", m.group(1) or "").strip()
    cur = (m.group(2) or "").upper().replace("LEI", "RON").replace("EURO", "€")
    if cur in ["E", "EURO"]: cur = "€"
    return val, cur or ""

# --------- PROXY ----------
@dataclass
class ProxyEndpoint:
    protocol: str  # "http" sau "socks5"
    host: str
    port: int
    username: str = ""
    password: str = ""

def pick_ua_profile() -> dict:
    return random.choice(UA_POOL)

def build_wire_options(ep: Optional[ProxyEndpoint]) -> Optional[dict]:
    if not ep: return None
    prot = ep.protocol.lower().strip()
    auth = f"{ep.username}:{ep.password}@" if (ep.username or ep.password) else ""
    if prot == "http":
        opts = {"http": f"http://{auth}{ep.host}:{ep.port}",
                "https": f"https://{auth}{ep.host}:{ep.port}"}
    elif prot == "socks5":
        opts = {"http": f"socks5://{auth}{ep.host}:{ep.port}",
                "https": f"socks5://{auth}{ep.host}:{ep.port}"}
    else:
        raise ValueError(f"Protocol necunoscut: {ep.protocol}")
    out = {"proxy": opts}
    if not VERIFY_SSL:
        out["verify_ssl"] = False
    return out

def apply_stealth(driver, ua_profile: dict) -> None:
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'languages', {get: () => ['ro-RO','ro','en-US','en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'platform', {get: () => '%s'});
        """ % ua_profile.get("platform", "Win32")
    })

def make_driver(ep: Optional[ProxyEndpoint], ua_profile: Optional[dict] = None):
    if ua_profile is None:
        ua_profile = pick_ua_profile()
    width  = random.randint(*VIEWPORT_W)
    height = random.randint(*VIEWPORT_H)

    chrome_opts = wire_webdriver.ChromeOptions()
    if HEADLESS:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument(f"--window-size={width},{height}")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument(f"--lang={ua_profile['lang']}")
    chrome_opts.add_argument(f"--user-agent={ua_profile['ua']}")

    sw_options = build_wire_options(ep) if ep else None
    driver = wire_webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        seleniumwire_options=sw_options,
        options=chrome_opts
    )
    apply_stealth(driver, ua_profile)
    return driver

# --------- LOGIN OLX ----------
def is_logged_in(driver) -> bool:
    try:
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-user-name']"): return True
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-link']"): return True
        if driver.find_elements(By.XPATH, "//a[contains(., 'Contul meu') or contains(., 'Account')]"): return True
    except Exception:
        pass
    return False

def accept_cookies_if_any(driver) -> None:
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='cookies-popup-accept-all']"),
        (By.XPATH, "//button[contains(., 'Acceptă toate') or contains(., 'Accepta toate') or contains(., 'Accept all') or contains(., 'Accept All')]"),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollInto_
