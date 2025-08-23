"""
OLX – Scraper autorulote (Login OLX + GeoNode rotation + exponential backoff + stealth)
---------------------------------------------------------------------------------------
Colectează din fiecare anunț:
  telefon, titlu, pret, persoana, garantie, descriere, id_anunt, vizualizari, vanzator, url
și exportă în:
  - CSV (UTF-8 cu BOM, Excel-friendly)
  - XLSX (foaie 'anunturi', coloane curate)
  - (opțional) JSONL – convenabil pentru LLM-uri

Ce conține:
- Logare OLX (autologin din .env sau „asistat”: îți deschide pagina de login și așteaptă).
- Rotație între o LISTĂ de endpoint-uri GeoNode (round-robin).
- Retry cu backoff exponențial pentru paginile de listă și pentru fiecare anunț;
  la fiecare încercare schimbă endpointul + user-agent + viewport.
- „Stealth hardening”: user-agent pool (Chrome/Edge Win/Mac), limbă ro-RO, viewport random,
  mascarea navigator.webdriver, simulare window.chrome, plugins/languages.

Bune practici:
- Rulează inițial FĂRĂ headless ca să vezi browserul și să te poți loga/rezolva eventuale verificări.
- Respectă Termenii OLX; nu folosi datele pentru spam.

Dependințe:
  pip install selenium webdriver-manager selenium-wire python-dotenv pandas openpyxl
"""

import csv
import json
import os
import random
import re
import time
import unicodedata
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
from dotenv import load_dotenv

# =========================
# CONFIG GENERAL
# =========================
SEARCH_URL = "https://www.olx.ro/oferte/q-inchiriere-autorulota/"

# modul de rotație proxy: driver nou per pagină sau per anunț
ROTATE_PER = "page"   # "page" sau "ad"

# retry & backoff
MAX_PAGE_RETRIES = 3
MAX_AD_RETRIES   = 3
BACKOFF_BASE_SEC = 1.0     # secunde pentru prima întârziere
BACKOFF_FACTOR   = 2.0     # exponențial: base * (factor ** (attempt-1))
BACKOFF_JITTER   = 0.35    # +/- jitter relativ

# headless: recomand False la început (mai „uman” și ușor de depanat)
HEADLESS = False
# viewport aleator
VIEWPORT_W = (1200, 1920)
VIEWPORT_H = (740, 1080)

# întârzieri „umane”
SLEEP_NAV_MIN = 0.25
SLEEP_NAV_MAX = 0.6
SLEEP_BETWEEN_ADS = 0.8

# paginare: ne oprim dacă două pagini consecutive nu au anunțuri
MAX_EMPTY_PAGES_BEFORE_STOP = 2

# login OLX
REQUIRE_LOGIN = True            # dacă vrei să fii logat (recomandat)
ASSISTED_LOGIN_TIMEOUT = 90     # secunde de așteptare pentru logare manuală (dacă autologin eșuează)
COOKIES_SHARE_BETWEEN_DRIVERS = True  # încearcă să reutilizezi cookie-urile între drivere (poate fi invalidat de IP/UA diferit)

# export
EXPORT_JSONL = True   # util pentru LLM
OUTPUT_PREFIX = "anunturi_autorulote"  # fișierele vor fi: <prefix>_YYYYMMDD-HHMMSS.*

GEONODE_LINES = [
    "proxy.geonode.io:9000:geonode_3UlswT3blD-type-residential:41a8aed7-f884-4940-b016-af7c82c684a8",
    # poți pune aici și alte linii (dacă sunt diferite)
]

ENDPOINTS = []
for line in GEONODE_LINES:
    host, port, user, pwd = line.strip().split(":", 3)
    ENDPOINTS.append({
        "protocol": "http",      # pentru HTTP/HTTPS
        "host": host,
        "port": int(port),
        "username": geonode_3UlswT3blD,
        "password": 41a8aed7-f884-4940-b016-af7c82c684a8,
    })


# Fallback .env dacă lista e goală
load_dotenv()
if not ENDPOINTS:
    ENV_USERNAME = os.getenv("GEONODE_USERNAME", "").strip()
    ENV_PASSWORD = os.getenv("GEONODE_PASSWORD", "").strip()
    ENV_HOST     = os.getenv("GEONODE_HOST", "proxy.geonode.io").strip()
    ENV_PORT     = int(os.getenv("GEONODE_PORT", "9000").strip())
    ENV_PROTOCOL = os.getenv("GEONODE_PROTOCOL", "http").strip()
    if ENV_HOST and ENV_PORT:
        ENDPOINTS = [{
            "protocol": ENV_PROTOCOL,
            "host": ENV_HOST,
            "port": ENV_PORT,
            "username": ENV_USERNAME,
            "password": ENV_PASSWORD,
        }]

USE_PROXY = len(ENDPOINTS) > 0

# credențiale OLX (pentru autologin)
OLX_EMAIL    = os.getenv("OLX_EMAIL", "").strip()
OLX_PASSWORD = os.getenv("OLX_PASSWORD", "").strip()

# =========================
# USER-AGENT POOL (simulăm browsere diferite)
# =========================
UA_POOL = [
    # Chrome Windows
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "Win32"},
    # Edge Windows
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "Win32"},
    # Chrome macOS
    {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "MacIntel"},
]

# =========================
# REGEX & UTILE
# =========================
PHONE_RE = re.compile(
    r"(?:\+?4?0\s*7[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
    r"|(?:07[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
)
RE_ID = re.compile(r"\bID(?:-ul)?(?:\s*anuntului)?\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
RE_VIEWS = re.compile(r"\bVizualizari\s*[:#]?\s*([\d\s\.]+)", re.IGNORECASE)
RE_GARANTIE = re.compile(r"\bGarantie\b.*?[:\-]?\s*([\d\s\.]+(?:\s*(?:RON|Lei|EUR|€))?)", re.IGNORECASE)

def jitter_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

def exp_backoff_sleep(attempt: int) -> None:
    """Exponential backoff cu jitter: base * factor^(attempt-1) +/- jitter."""
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
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )

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
    """Compactează spațiile și elimină caractere de control – util pt. export/LLM."""
    if not s: return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_price(raw: str) -> Tuple[str, str]:
    """Extrage valoarea numerică și moneda dintr-un text de preț (ex. '109 €')."""
    if not raw: return "", ""
    m = re.search(r"([\d\.\s]+)\s*([€EeUuRrOo]|RON|Lei|LEI)?", raw)
    if not m: return "", ""
    val = re.sub(r"[^\d]", "", m.group(1) or "").strip()
    cur = (m.group(2) or "").upper().replace("LEI", "RON").replace("EURO", "€")
    if cur in ["E", "EURO"]: cur = "€"
    return val, cur or ""

# =========================
# DRIVER & PROXY
# =========================
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
        return {"proxy": {"http": f"http://{auth}{ep.host}:{ep.port}",
                          "https": f"https://{auth}{ep.host}:{ep.port}"}}
    elif prot == "socks5":
        return {"proxy": {"http": f"socks5://{auth}{ep.host}:{ep.port}",
                          "https": f"socks5://{auth}{ep.host}:{ep.port}"}}
    else:
        raise ValueError(f"Protocol necunoscut pentru proxy: {ep.protocol}")

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
    if ep:
        print(f"[DEBUG] Proxy: {ep.protocol}://{ep.host}:{ep.port} | UA={ua_profile['ua'][:35]}...")
    else:
        print(f"[DEBUG] Fără proxy | UA={ua_profile['ua'][:35]}...")
    return driver

# =========================
# LOGIN OLX
# =========================
def is_logged_in(driver) -> bool:
    try:
        # Caută indicii de cont/logare: nume utilizator sau meniu cont
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-user-name']"): return True
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-link']"): return True
        if driver.find_elements(By.XPATH, "//a[contains(., 'Contul meu') or contains(., 'Account')]"): return True
    except Exception:
        pass
    return False

def add_cookies_if_any(driver, cookies_store: List[dict]) -> None:
    if not cookies_store: return
    try:
        driver.get("https://www.olx.ro/")
        for ck in cookies_store:
            try:
                driver.add_cookie(ck)
            except Exception:
                continue
        driver.get("https://www.olx.ro/")
    except Exception:
        pass

def olx_login_if_needed(driver, cookies_store: List[dict]) -> List[dict]:
    """
    1) Dacă avem cookies salvate, le injectăm.
    2) Dacă nu ești logat:
         - dacă OLX_EMAIL/PASSWORD sunt setate, încearcă autologin;
         - altfel oferă asistență: deschide pagina de login și așteaptă ASSISTED_LOGIN_TIMEOUT secunde.
    Returnează cookies salvate (pentru reutilizare între drivere).
    """
    # Reutilizare cookies (dacă e activată)
    if COOKIES_SHARE_BETWEEN_DRIVERS and cookies_store:
        add_cookies_if_any(driver, cookies_store)
        if is_logged_in(driver):
            print("[DEBUG] OLX: logat prin cookies reuse.")
            return cookies_store

    # Verificare rapidă
    driver.get("https://www.olx.ro/")
    accept_cookies_if_any(driver)
    if is_logged_in(driver):
        print("[DEBUG] OLX: deja logat.")
        if COOKIES_SHARE_BETWEEN_DRIVERS and not cookies_store:
            cookies_store[:] = driver.get_cookies()
        return cookies_store

    # Încearcă autologin dacă avem credențiale
    if OLX_EMAIL and OLX_PASSWORD:
        try:
            driver.get("https://www.olx.ro/cont/")
            accept_cookies_if_any(driver)
            time.sleep(1.0)

            # încercăm să găsim câmpurile (formula tolerantă)
            email_candidates = [
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[name='email']"),
                (By.CSS_SELECTOR, "input[name='username']"),
                (By.XPATH, "//input[contains(@placeholder, 'Email') or contains(@placeholder, 'email') or contains(@placeholder, 'Telefon')]"),
            ]
            pwd_candidates = [
                (By.CSS_SELECTOR, "input[type='password']"),
                (By.CSS_SELECTOR, "input[name='password']"),
                (By.XPATH, "//input[contains(@placeholder, 'Parol') or contains(@placeholder, 'password')]"),
            ]
            submit_candidates = [
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//button[contains(., 'Autentificare') or contains(., 'Conectare') or contains(., 'Log in') or contains(., 'Login')]"),
            ]

            # email/phone
            email_box = None
            for by, sel in email_candidates:
                try:
                    email_box = WebDriverWait(driver, 6).until(EC.presence_of_element_located((by, sel)))
                    break
                except Exception:
                    continue
            if email_box:
                email_box.clear(); email_box.send_keys(OLX_EMAIL)
                time.sleep(0.3)

            # password
            pwd_box = None
            for by, sel in pwd_candidates:
                try:
                    pwd_box = driver.find_element(by, sel)
                    break
                except Exception:
                    continue
            if pwd_box:
                pwd_box.clear(); pwd_box.send_keys(OLX_PASSWORD)
                time.sleep(0.2)
                pwd_box.send_keys(Keys.ENTER)

            # submit (fallback explicit)
            if not pwd_box:
                for by, sel in submit_candidates:
                    try:
                        btn = driver.find_element(by, sel)
                        btn.click(); break
                    except Exception:
                        continue

            # așteptăm starea „logat”
            for _ in range(15):
                if is_logged_in(driver):
                    print("[DEBUG] OLX: autologin reușit.")
                    if COOKIES_SHARE_BETWEEN_DRIVERS:
                        cookies_store[:] = driver.get_cookies()
                    return cookies_store
                time.sleep(1.0)

            print("[WARN] Autologin nereușit. Trec pe logare asistată.")
        except Exception as e:
            print(f"[WARN] Autologin a eșuat cu excepție: {e}")

    # Logare asistată: deschide pagina & așteaptă să te loghezi manual
    try:
        driver.get("https://www.olx.ro/cont/")
        accept_cookies_if_any(driver)
        print(f"[INFO] Te rog loghează-te manual în fereastra OLX (ai ~{ASSISTED_LOGIN_TIMEOUT}s).")
        t0 = time.time()
        while time.time() - t0 < ASSISTED_LOGIN_TIMEOUT:
            if is_logged_in(driver):
                print("[DEBUG] OLX: logare manuală reușită.")
                if COOKIES_SHARE_BETWEEN_DRIVERS:
                    cookies_store[:] = driver.get_cookies()
                break
            time.sleep(1.5)
    except Exception:
        pass

    return cookies_store

# =========================
# PAGINA DE LISTĂ & ANUNȚ
# =========================
def accept_cookies_if_any(driver) -> None:
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='cookies-popup-accept-all']"),
        (By.XPATH, "//button[contains(., 'Acceptă toate') or contains(., 'Accepta toate') or contains(., 'Accept all') or contains(., 'Accept All')]"),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            jitter_sleep(0.1, 0.2)
            btn.click()
            jitter_sleep(0.2, 0.3)
            return
        except Exception:
            continue

def wait_for_listings(driver) -> None:
    try:
        WebDriverWait(driver, 12).until(
            EC.any_of(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-cy='l-card']")),
                EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Nu am găsit anunțuri') or contains(., 'No results')]")),
            )
        )
    except Exception:
        pass

def collect_listing_links_from_page(driver) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    seen = set()
    cards = driver.find_elements(By.CSS_SELECTOR, "[data-cy='l-card']")
    for c in cards:
        a = None
        try:
            a = c.find_element(By.CSS_SELECTOR, "a[href*='/d/oferta/']")
        except Exception:
            try:
                a = c.find_element(By.XPATH, ".//a[contains(@href, '/d/oferta/')]")
            except Exception:
                a = None
        if not a:
            continue
        href = a.get_attribute("href")
        title = text_or_empty(a)
        if href and href not in seen:
            seen.add(href)
            links.append((title, href))
    return links

def reveal_and_extract_phone_on_ad(driver) -> List[str]:
    phones = set()
    try:
        driver.execute_script("window.scrollTo(0, 300);")
        jitter_sleep(0.2, 0.3)
    except Exception:
        pass

    # 1) din text
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        for m in PHONE_RE.findall(body_text):
            m = m if isinstance(m, str) else next(filter(None, m), "")
            if m: phones.add(clean_phone(m))
    except Exception:
        pass

    # 2) butoane 'Arată/Show'
    btn_selectors = [
        (By.CSS_SELECTOR, "[data-testid='show-phone-number']"),
        (By.CSS_SELECTOR, "[data-cy='ad-contact-phone']"),
        (By.XPATH, "//button[contains(., 'Arată') or contains(., 'Arata') or contains(., 'Show')]"),
        (By.XPATH, "//*[self::button or self::a][contains(., 'Arată') or contains(., 'Arata') or contains(., 'Show')]"),
        (By.XPATH, "//*[contains(., 'Trimite mesaj')]/following::button[1]"),
    ]
    if not phones:
        for by, sel in btn_selectors:
            try:
                btn = WebDriverWait(driver, 4).until(EC.element_to_be_clickable((by, sel)))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                jitter_sleep(0.2, 0.3)
                try: btn.click()
                except Exception: driver.execute_script("arguments[0].click();", btn)
                jitter_sleep(1.0, 1.2)
                body_text = driver.find_element(By.TAG_NAME, "body").text
                for m in PHONE_RE.findall(body_text):
                    m = m if isinstance(m, str) else next(filter(None, m), "")
                    if m: phones.add(clean_phone(m))
                if phones: break
            except Exception:
                continue

    if not phones:
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 600);")
            jitter_sleep(0.5, 0.7)
            body_text = driver.find_element(By.TAG_NAME, "body").text
            for m in PHONE_RE.findall(body_text):
                m = m if isinstance(m, str) else next(filter(None, m), "")
                if m: phones.add(clean_phone(m))
        except Exception:
            pass

    return [p for p in phones if p.startswith("07") and len(p) == 10]

def extract_ad_fields(driver) -> Dict[str, str]:
    titlu = first_text_by_selectors(driver, [
        (By.CSS_SELECTOR, "[data-cy='offer_title'] h1, [data-cy='offer_title'] h4"),
        (By.CSS_SELECTOR, "h1[data-cy='ad_title']"),
        (By.CSS_SELECTOR, "[data-testid='offer_title'] h1, [data-testid='offer_title'] h4"),
    ])
    pret = first_text_by_selectors(driver, [
        (By.CSS_SELECTOR, "[data-testid='ad-price-container']"),
        (By.XPATH, "//*[self::h3 or self::h2][contains(., '€') or contains(., 'Lei') or contains(., 'RON')]"),
    ])
    persoana = first_text_by_selectors(driver, [
        (By.CSS_SELECTOR, "[data-testid='user-type']"),
        (By.XPATH, "//p[contains(., 'Persoana') or contains(., 'Persoană') or contains(., 'Firm')]"),
        (By.CSS_SELECTOR, "p.css-5l1a1j span"),
    ])
    vanzator = first_text_by_selectors(driver, [
        (By.CSS_SELECTOR, "[data-testid='user-profile-user-name']"),
        (By.CSS_SELECTOR, "[data-testid='user-profile-link']"),
        (By.XPATH, "//h4[contains(@data-testid, 'user-profile-user-name')]"),
    ])
    descriere = first_text_by_selectors(driver, [
        (By.CSS_SELECTOR, "[data-testid='ad_description']"),
        (By.CSS_SELECTOR, "[data-cy='ad_description']"),
    ])

    # curățări/mining suplimentare
    descriere = sanitize_text(descriere)
    pret_val, pret_cur = parse_price(pret)

    try:
        body_text_raw = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body_text_raw = ""
    body_norm = strip_diacritics(body_text_raw)

    garantie = ""
    m = RE_GARANTIE.search(body_norm)
    if m: garantie = sanitize_text(m.group(1))

    id_anunt = ""
    m = RE_ID.search(body_norm)
    if m: id_anunt = sanitize_text(m.group(1))

    vizualizari = ""
    m = RE_VIEWS.search(body_norm)
    if m: vizualizari = sanitize_text(m.group(1))

    return {
        "titlu": sanitize_text(titlu),
        "pret": sanitize_text(pret),
        "pret_valoare": pret_val,
        "pret_moneda": pret_cur,
        "persoana": sanitize_text(persoana),
        "garantie": sanitize_text(garantie),
        "descriere": descriere,
        "id_anunt": id_anunt,
        "vizualizari": vizualizari,
        "vanzator": sanitize_text(vanzator),
    }

# =========================
# RETRY HELPERS (cu backoff exponențial)
# =========================
def try_get_listings_with_retries(rr, page_url: str, attempts: int, cookies_store: List[dict]) -> List[Tuple[str, str]]:
    last_err = None
    for i in range(1, attempts + 1):
        ep = next(rr) if USE_PROXY else None
        ua = pick_ua_profile()
        driver = make_driver(ep, ua)
        try:
            # (opțional) logare OLX
            if REQUIRE_LOGIN:
                olx_login_if_needed(driver, cookies_store)

            driver.get(page_url)
            accept_cookies_if_any(driver)
            wait_for_listings(driver)
            jitter_sleep(SLEEP_NAV_MIN, SLEEP_NAV_MAX)
            links = collect_listing_links_from_page(driver)
            print(f"[DEBUG] page attempt {i}/{attempts}: {len(links)} anunțuri")
            if links:
                return links
        except Exception as e:
            last_err = e
            print(f"[DEBUG] page attempt {i} error: {e}")
        finally:
            driver.quit()

        exp_backoff_sleep(i)

    if last_err:
        print(f"[WARN] Eșec la {page_url}: {last_err}")
    return []

def try_process_ad_with_retries(rr, href: str, attempts: int, cookies_store: List[dict]) -> Tuple[Dict[str, str], List[str]]:
    last_err = None
    for i in range(1, attempts + 1):
        ep = next(rr) if USE_PROXY else None
        ua = pick_ua_profile()
        driver = make_driver(ep, ua)
        try:
            # (opțional) logare OLX
            if REQUIRE_LOGIN:
                olx_login_if_needed(driver, cookies_store)

            driver.get(href)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            jitter_sleep(0.4, 0.6)
            accept_cookies_if_any(driver)

            fields = extract_ad_fields(driver)
            phones = reveal_and_extract_phone_on_ad(driver)

            print(f"[DEBUG] ad attempt {i}/{attempts}: phones={len(phones)}")
            if fields.get("titlu") or phones:
                driver.quit()
                return fields, phones
        except Exception as e:
            last_err = e
            print(f"[DEBUG] ad attempt {i} error: {e}")
        finally:
            try: driver.quit()
            except Exception: pass

        exp_backoff_sleep(i)

    if last_err:
        print(f"[WARN] Eșec la {href}: {last_err}")
    return {
        "titlu": "", "pret": "", "pret_valoare": "", "pret_moneda": "",
        "persoana": "", "garantie": "", "descriere": "",
        "id_anunt": "", "vizualizari": "", "vanzator": "",
    }, []

# =========================
# MAIN
# =========================
def main():
    proxies = [ProxyEndpoint(**ep) for ep in ENDPOINTS] if USE_PROXY else [None]
    rr = cycle(proxies)

    all_rows: List[Dict[str, str]] = []
    seen_phones = set()
    visited_ads = 0
    empty_pages = 0
    page_index = 1
    cookies_store: List[dict] = []  # stocăm cookie-urile (dacă alegem să le reutilizăm)

    try:
        while True:
            page_url = SEARCH_URL if page_index == 1 else f"{SEARCH_URL}?page={page_index}"

            # === Retry pentru pagina de listă (cu backoff exponențial) ===
            listings = try_get_listings_with_retries(rr, page_url, MAX_PAGE_RETRIES, cookies_store)
            print(f"[DEBUG] Pagina {page_index}: {len(listings)} anunțuri găsite")

            if not listings:
                empty_pages += 1
                if empty_pages >= MAX_EMPTY_PAGES_BEFORE_STOP:
                    print(f"[DEBUG] Oprire: {empty_pages} pagini consecutive fără anunțuri.")
                    break
            else:
                empty_pages = 0

            # === Parcurgem anunțurile (retry per anunț) ===
            for _, href in listings:
                fields, phones = try_process_ad_with_retries(rr, href, MAX_AD_RETRIES, cookies_store)
                visited_ads += 1

                if phones:
                    for ph in phones:
                        if ph not in seen_phones:
                            seen_phones.add(ph)
                            all_rows.append({
                                "telefon": ph,
                                "titlu": fields["titlu"], "pret": fields["pret"],
                                "pret_valoare": fields["pret_valoare"], "pret_moneda": fields["pret_moneda"],
                                "persoana": fields["persoana"], "garantie": fields["garantie"],
                                "descriere": fields["descriere"], "id_anunt": fields["id_anunt"],
                                "vizualizari": fields["vizualizari"], "vanzator": fields["vanzator"],
                                "url": href
                            })
                else:
                    all_rows.append({
                        "telefon": "",
                        "titlu": fields["titlu"], "pret": fields["pret"],
                        "pret_valoare": fields["pret_valoare"], "pret_moneda": fields["pret_moneda"],
                        "persoana": fields["persoana"], "garantie": fields["garantie"],
                        "descriere": fields["descriere"], "id_anunt": fields["id_anunt"],
                        "vizualizari": fields["vizualizari"], "vanzator": fields["vanzator"],
                        "url": href
                    })

                time.sleep(SLEEP_BETWEEN_ADS)

            page_index += 1

        # === Salvare cu timestamp ===
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_path  = f"{OUTPUT_PREFIX}_{timestamp}.csv"
        xlsx_path = f"{OUTPUT_PREFIX}_{timestamp}.xlsx"
        jsonl_path= f"{OUTPUT_PREFIX}_{timestamp}.jsonl"

        # dedup (telefon + url)
        dedup = {}
        for row in all_rows:
            key = (row.get("telefon",""), row.get("url",""))
            dedup[key] = row
        rows = list(dedup.values())

        # sort: întâi cele cu telefon, apoi după telefon/titlu
        rows.sort(key=lambda r: (r.get("telefon","") == "", r.get("telefon",""), r.get("titlu","")))

        # CSV (UTF-8 BOM)
        fieldnames = [
            "telefon", "titlu", "pret", "pret_valoare", "pret_moneda",
            "persoana", "garantie", "descriere", "id_anunt", "vizualizari", "vanzator", "url"
        ]
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        # XLSX
        df = pd.DataFrame(rows, columns=fieldnames)
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="anunturi")

        # JSONL (opțional, util pentru LLM ingest)
        if EXPORT_JSONL:
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")

        found = sum(1 for r in rows if r.get("telefon"))
        print(f"\nOK. Am salvat {found} numere (unice).")
        print(f"CSV : {csv_path}")
        print(f"XLSX: {xlsx_path}")
        if EXPORT_JSONL:
            print(f"JSONL: {jsonl_path}")
        print(f"Anunțuri vizitate: {visited_ads}. Total rânduri: {len(rows)}.")

    finally:
        # driverele sunt create/închise în funcțiile de retry
        pass


if __name__ == "__main__":
    main()
