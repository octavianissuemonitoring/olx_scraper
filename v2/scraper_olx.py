# scraper_olx.py
"""
OLX Scraper – configurabil prin fișiere externe (proxies.json, secrets.env, urls.txt)
------------------------------------------------------------------------------------
- Pool-uri separate de proxie: LIST (rotating, RO/BG/HU/PL/DE) și AD (sticky RO, 2 sesiuni).
- Login OLX (auto din .env; fallback logare asistată în fereastră).
- Retry cu backoff exponențial, rotație UA + endpoint la fiecare încercare.
- Circuit-breaker minimal pe endpoint-uri (scor negativ => cooldown temporar).
- Măsuri „stealth” (UA pool, limbă ro-RO, navigator.webdriver mascat).
- Export CSV (UTF-8 BOM), XLSX (openpyxl) și JSONL (LLM-friendly), cu timestamp.

Rulare:
  py scraper_olx.py

Folder:
  proxies.json   – setări proxy (vezi exemplu)
  secrets.env    – OLX_EMAIL / OLX_PASSWORD
  urls.txt       – lista de URL-uri de start (câte una pe linie)
"""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import time
import unicodedata
from dataclasses import dataclass, field
from itertools import cycle
from typing import Dict, List, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv
from seleniumwire import webdriver as wire_webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("olx")

# =========================
# CONFIG „STATIC” (ajustezi ușor aici)
# =========================
OUTPUT_PREFIX = "anunturi_autorulote"
EXPORT_JSONL = True
HEADLESS = False  # pentru depănare, lasă False
MAX_EMPTY_PAGES_BEFORE_STOP = 2
MAX_PAGES_PER_SEED = None  # ex. 10 ca limită hard; None = no limit

# retry/backoff
MAX_PAGE_RETRIES = 3
MAX_AD_RETRIES = 3
BACKOFF_BASE_SEC = 1.0
BACKOFF_FACTOR = 2.0
BACKOFF_JITTER = 0.35

# ritm „uman”
SLEEP_NAV_MIN = 0.25
SLEEP_NAV_MAX = 0.6
SLEEP_BETWEEN_ADS = 0.9

# login & cookies
REQUIRE_LOGIN = True
ASSISTED_LOGIN_TIMEOUT = 90
COOKIES_SHARE_BETWEEN_DRIVERS = True

# viewport & UA
VIEWPORT_W = (1200, 1920)
VIEWPORT_H = (740, 1080)
UA_POOL = [
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "Win32"},
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "Win32"},
    {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7", "platform": "MacIntel"},
]

# regex utile
PHONE_RE = re.compile(
    r"(?:\+?4?0\s*7[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
    r"|(?:07[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
)
RE_ID = re.compile(r"\bID(?:-ul)?(?:\s*anuntului)?\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
RE_VIEWS = re.compile(r"\bVizualizari\s*[:#]?\s*([\d\s\.]+)", re.IGNORECASE)
RE_GARANTIE = re.compile(r"\bGarantie\b.*?[:\-]?\s*([\d\s\.]+(?:\s*(?:RON|Lei|EUR|€))?)", re.IGNORECASE)

# =========================
# DATE DIN FIȘIERE EXTERNE
# =========================
def read_urls(path: str = "urls.txt") -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lipsește {path}. Creează-l cu URL-urile de start (câte unul pe linie).")
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if u and not u.startswith("#"):
                urls.append(u)
    if not urls:
        raise ValueError(f"{path} este gol.")
    return urls

@dataclass
class ProxyEndpoint:
    protocol: str  # "http" sau "socks5"
    host: str
    port: int
    username: str = ""
    password: str = ""

@dataclass
class EndpointState:
    ep: ProxyEndpoint
    score: int = 0
    cooldown_until: float = 0.0  # epoch seconds

@dataclass
class ProxyPools:
    verify_ssl: bool
    list_pool: List[EndpointState] = field(default_factory=list)
    ad_pool: List[EndpointState] = field(default_factory=list)

def load_proxies(path: str = "proxies.json") -> ProxyPools:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lipsește {path}. Vezi exemplul din mesaj.")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    verify_ssl = bool(data.get("verify_ssl", True))
    def to_state_list(key: str) -> List[EndpointState]:
        out: List[EndpointState] = []
        for ep in data.get(key, []):
            try:
                out.append(EndpointState(ProxyEndpoint(
                    protocol=str(ep["protocol"]).strip(),
                    host=str(ep["host"]).strip(),
                    port=int(ep["port"]),
                    username=str(ep.get("username", "")),
                    password=str(ep.get("password", "")),
                )))
            except Exception as e:
                raise ValueError(f"Endpoint invalid în {key}: {ep} | {e}")
        return out

    list_pool = to_state_list("list_endpoints")
    ad_pool = to_state_list("ad_endpoints")
    if not list_pool:
        log.warning("Nu ai definit list_endpoints în proxies.json – vei rula fără proxy pe paginile de listă.")
    if not ad_pool:
        log.warning("Nu ai definit ad_endpoints în proxies.json – vei rula fără proxy pe anunț/login.")
    return ProxyPools(verify_ssl, list_pool, ad_pool)

def load_secrets(path: str = "secrets.env") -> Tuple[str, str]:
    load_dotenv(path)
    email = os.getenv("OLX_EMAIL", "").strip()
    password = os.getenv("OLX_PASSWORD", "").strip()
    if REQUIRE_LOGIN and (not email or not password):
        log.warning("OLX_EMAIL/OLX_PASSWORD nu sunt setate în secrets.env – se va cere logare manuală.")
    return email, password

# =========================
# HELPERI UTILI
# =========================
def jitter_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

def exp_backoff_sleep(attempt: int) -> None:
    base = BACKOFF_BASE_SEC * (BACKOFF_FACTOR ** (attempt - 1))
    jitter = base * BACKOFF_JITTER
    time.sleep(max(0.05, random.uniform(base - jitter, base + jitter)))

def clean_phone(s: str) -> str:
    digits = re.sub(r"\D", "", s)
    if digits.startswith("40") and len(digits) == 11 and digits[2] == "7":
        digits = "0" + digits[2:]
    if digits.startswith("7") and len(digits) == 9:
        digits = "0" + digits
    return digits

def strip_diacritics(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

def sanitize_text(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def parse_price(raw: str) -> Tuple[str, str]:
    if not raw: return "", ""
    m = re.search(r"([\d\.\s]+)\s*([€EeUuRrOo]|RON|Lei|LEI)?", raw)
    if not m: return "", ""
    val = re.sub(r"[^\d]", "", m.group(1) or "").strip()
    cur = (m.group(2) or "").upper().replace("LEI", "RON").replace("EURO", "€")
    if cur in ["E", "EURO"]: cur = "€"
    return val, cur or ""

def first_text_by_selectors(driver, selectors: List[Tuple[str, str]]) -> str:
    for by, sel in selectors:
        try:
            el = driver.find_element(by, sel)
            t = el.text.strip()
            if t: return t
        except Exception:
            continue
    return ""

# =========================
# PROXY ENGINE + CIRCUIT-BREAKER
# =========================
def build_wire_options(ep: Optional[ProxyEndpoint], verify_ssl: bool) -> Optional[dict]:
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
    if not verify_ssl:
        out["verify_ssl"] = False
    return out

def pick_ua_profile() -> dict:
    return random.choice(UA_POOL)

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

def make_driver(ep: Optional[ProxyEndpoint], verify_ssl: bool, ua_profile: Optional[dict] = None):
    if ua_profile is None:
        ua_profile = pick_ua_profile()
    width = random.randint(*VIEWPORT_W)
    height = random.randint(*VIEWPORT_H)

    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument(f"--window-size={width},{height}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--lang={ua_profile['lang']}")
    opts.add_argument(f"--user-agent={ua_profile['ua']}")

    sw_options = build_wire_options(ep, verify_ssl) if ep else None
    driver = wire_webdriver.Chrome(seleniumwire_options=sw_options, options=opts)
    apply_stealth(driver, ua_profile)
    return driver

def select_endpoint(pools: ProxyPools, pool_name: str, rr_iters: Dict[str, cycle]) -> Optional[EndpointState]:
    pool = pools.list_pool if pool_name == "list" else pools.ad_pool
    if not pool: return None
    if pool_name not in rr_iters:
        rr_iters[pool_name] = cycle(pool)
    # încercăm până găsim unul care nu e în cooldown
    for _ in range(len(pool)):
        cand: EndpointState = next(rr_iters[pool_name])
        if time.time() >= cand.cooldown_until:
            return cand
    return None  # toate în cooldown

def penalize_endpoint(es: Optional[EndpointState], reason: str, severe: bool = False) -> None:
    if not es: return
    delta = -2 if severe else -1
    es.score += delta
    cooldown = 60 if severe else 20
    es.cooldown_until = time.time() + cooldown
    log.debug(f"Endpoint penalizat ({reason}): {es.ep.host}:{es.ep.port} score={es.score} cooldown={cooldown}s")

def reward_endpoint(es: Optional[EndpointState]) -> None:
    if not es: return
    es.score = min(es.score + 1, 5)

# =========================
# LOGIN & NAVIGAȚIE
# =========================
def accept_cookies_if_any(driver) -> None:
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='cookies-popup-accept-all']"),
        (By.XPATH, "//button[contains(., 'Acceptă toate') or contains(., 'Accepta toate') or contains(., 'Accept all')]"),
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

def is_logged_in(driver) -> bool:
    try:
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-user-name']"): return True
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-link']"): return True
        if driver.find_elements(By.XPATH, "//a[contains(., 'Contul meu') or contains(., 'Account')]"): return True
    except Exception:
        pass
    return False

def reuse_cookies(driver, cookies_store: List[dict]) -> None:
    if not cookies_store: return
    try:
        driver.get("https://www.olx.ro/")
        for ck in cookies_store:
            try: driver.add_cookie(ck)
            except Exception: pass
        driver.get("https://www.olx.ro/")
    except Exception:
        pass

def olx_login_if_needed(driver, cookies_store: List[dict], email: str, password: str) -> List[dict]:
    if COOKIES_SHARE_BETWEEN_DRIVERS and cookies_store:
        reuse_cookies(driver, cookies_store)
        if is_logged_in(driver):
            return cookies_store

    driver.get("https://www.olx.ro/")
    accept_cookies_if_any(driver)
    if is_logged_in(driver):
        if COOKIES_SHARE_BETWEEN_DRIVERS and not cookies_store:
            cookies_store[:] = driver.get_cookies()
        return cookies_store

    if email and password:
        try:
            driver.get("https://www.olx.ro/cont/")
            accept_cookies_if_any(driver)
            time.sleep(1.0)

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

            email_box = None
            for by, sel in email_candidates:
                try:
                    email_box = WebDriverWait(driver, 6).until(EC.presence_of_element_located((by, sel)))
                    break
                except Exception:
                    continue
            if email_box:
                email_box.clear(); email_box.send_keys(email); time.sleep(0.3)

            pwd_box = None
            for by, sel in pwd_candidates:
                try:
                    pwd_box = driver.find_element(by, sel); break
                except Exception:
                    continue
            if pwd_box:
                pwd_box.clear(); pwd_box.send_keys(password); time.sleep(0.2); pwd_box.send_keys(Keys.ENTER)

            for _ in range(15):
                if is_logged_in(driver):
                    if COOKIES_SHARE_BETWEEN_DRIVERS:
                        cookies_store[:] = driver.get_cookies()
                    return cookies_store
                time.sleep(1.0)
        except Exception:
            pass

    # asistat: dă-ți timp să te loghezi manual
    try:
        driver.get("https://www.olx.ro/cont/")
        accept_cookies_if_any(driver)
        log.info(f"Logare manuală OLX: ai ~{ASSISTED_LOGIN_TIMEOUT}s în fereastră.")
        t0 = time.time()
        while time.time() - t0 < ASSISTED_LOGIN_TIMEOUT:
            if is_logged_in(driver):
                if COOKIES_SHARE_BETWEEN_DRIVERS:
                    cookies_store[:] = driver.get_cookies()
                break
            time.sleep(1.5)
    except Exception:
        pass
    return cookies_store

# =========================
# EXTRACȚIE DATE ANUNȚ
# =========================
def reveal_and_extract_phone_on_ad(driver) -> List[str]:
    phones = set()
    try:
        driver.execute_script("window.scrollTo(0, 300);"); jitter_sleep(0.2, 0.3)
    except Exception:
        pass
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        for m in PHONE_RE.findall(body_text):
            m = m if isinstance(m, str) else next(filter(None, m), "")
            if m: phones.add(clean_phone(m))
    except Exception:
        pass

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
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 600);"); jitter_sleep(0.5, 0.7)
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
# PAGE FLOWS + RETRY
# =========================
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
    for c in driver.find_elements(By.CSS_SELECTOR, "[data-cy='l-card']"):
        a = None
        try:
            a = c.find_element(By.CSS_SELECTOR, "a[href*='/d/oferta/']")
        except Exception:
            try:
                a = c.find_element(By.XPATH, ".//a[contains(@href, '/d/oferta/')]")
            except Exception:
                a = None
        if not a: continue
        href = a.get_attribute("href")
        title = a.text.strip() if a.text else ""
        if href and href not in seen:
            seen.add(href)
            links.append((title, href))
    return links

def try_get_listings_with_retries(pools: ProxyPools, rr_iters: Dict[str, cycle], url: str, attempts: int,
                                  cookies_store: List[dict], email: str, password: str) -> List[Tuple[str, str]]:
    last_err = None
    endpoint_used: Optional[EndpointState] = None
    for i in range(1, attempts + 1):
        endpoint_used = select_endpoint(pools, "list", rr_iters)
        ua = pick_ua_profile()
        driver = None
        try:
            driver = make_driver(endpoint_used.ep if endpoint_used else None, pools.verify_ssl, ua)
            if REQUIRE_LOGIN:
                olx_login_if_needed(driver, cookies_store, email, password)
            driver.get(url)
            accept_cookies_if_any(driver)
            wait_for_listings(driver)
            jitter_sleep(SLEEP_NAV_MIN, SLEEP_NAV_MAX)
            links = collect_listing_links_from_page(driver)
            log.info(f"[list] încercarea {i}/{attempts}: {len(links)} anunțuri de pe {url}")
            if links:
                reward_endpoint(endpoint_used)
                driver.quit()
                return links
            else:
                penalize_endpoint(endpoint_used, "no_links")
        except Exception as e:
            last_err = e
            log.warning(f"[list] eroare la încercarea {i}: {e}")
            penalize_endpoint(endpoint_used, "exception", severe=False)
        finally:
            try:
                if driver: driver.quit()
            except Exception:
                pass
        exp_backoff_sleep(i)
    if last_err:
        log.error(f"[list] Eșec la {url}: {last_err}")
    return []

def try_process_ad_with_retries(pools: ProxyPools, rr_iters: Dict[str, cycle], href: str, attempts: int,
                                cookies_store: List[dict], email: str, password: str):
    last_err = None
    endpoint_used: Optional[EndpointState] = None
    for i in range(1, attempts + 1):
        endpoint_used = select_endpoint(pools, "ad", rr_iters)
        ua = pick_ua_profile()
        driver = None
        try:
            driver = make_driver(endpoint_used.ep if endpoint_used else None, pools.verify_ssl, ua)
            if REQUIRE_LOGIN:
                olx_login_if_needed(driver, cookies_store, email, password)

            driver.get(href)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            jitter_sleep(0.4, 0.6)
            accept_cookies_if_any(driver)

            fields = extract_ad_fields(driver)
            phones = reveal_and_extract_phone_on_ad(driver)
            log.info(f"[ad] încercarea {i}/{attempts}: phones={len(phones)} | {href}")

            if fields.get("titlu") or phones:
                reward_endpoint(endpoint_used)
                driver.quit()
                return fields, phones
            else:
                penalize_endpoint(endpoint_used, "empty_fields")
        except Exception as e:
            last_err = e
            log.warning(f"[ad] eroare la încercarea {i}: {e}")
            penalize_endpoint(endpoint_used, "exception", severe=False)
        finally:
            try:
                if driver: driver.quit()
            except Exception:
                pass
        exp_backoff_sleep(i)
    if last_err:
        log.error(f"[ad] Eșec la {href}: {last_err}")
    return {
        "titlu": "", "pret": "", "pret_valoare": "", "pret_moneda": "",
        "persoana": "", "garantie": "", "descriere": "",
        "id_anunt": "", "vizualizari": "", "vanzator": "",
    }, []

# =========================
# MAIN
# =========================
def main():
    # 1) încarcă fișiere
    seeds = read_urls("urls.txt")
    pools = load_proxies("proxies.json")
    email, password = load_secrets("secrets.env")

    rr_iters: Dict[str, cycle] = {}

    all_rows: List[Dict[str, str]] = []
    seen_phones = set()
    visited_ads = 0

    try:
        for seed in seeds:
            log.info(f"=== Seed: {seed} ===")
            empty_pages = 0
            page_index = 1
            cookies_store: List[dict] = []

            while True:
                if MAX_PAGES_PER_SEED and page_index > MAX_PAGES_PER_SEED:
                    log.info(f"Limită MAX_PAGES_PER_SEED atinsă ({MAX_PAGES_PER_SEED}) pentru seed {seed}.")
                    break

                page_url = seed if page_index == 1 else f"{seed.rstrip('/')}/?page={page_index}"
                listings = try_get_listings_with_retries(
                    pools, rr_iters, page_url, MAX_PAGE_RETRIES, cookies_store, email, password
                )
                log.info(f"Pagina {page_index}: {len(listings)} anunțuri")

                if not listings:
                    empty_pages += 1
                    if empty_pages >= MAX_EMPTY_PAGES_BEFORE_STOP:
                        log.info(f"Oprire seed: {empty_pages} pagini consecutive fără anunțuri.")
                        break
                else:
                    empty_pages = 0

                for _, href in listings:
                    fields, phones = try_process_ad_with_retries(
                        pools, rr_iters, href, MAX_AD_RETRIES, cookies_store, email, password
                    )
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

        # 2) Salvare rezultate (CSV/XLSX/JSONL)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_path  = f"{OUTPUT_PREFIX}_{timestamp}.csv"
        xlsx_path = f"{OUTPUT_PREFIX}_{timestamp}.xlsx"
        jsonl_path= f"{OUTPUT_PREFIX}_{timestamp}.jsonl"

        # dedup pe (telefon, url)
        dedup = {}
        for row in all_rows:
            key = (row.get("telefon", ""), row.get("url", ""))
            dedup[key] = row
        rows = list(dedup.values())
        rows.sort(key=lambda r: (r.get("telefon","") == "", r.get("telefon",""), r.get("titlu","")))

        fieldnames = [
            "telefon", "titlu", "pret", "pret_valoare", "pret_moneda",
            "persoana", "garantie", "descriere", "id_anunt", "vizualizari", "vanzator", "url"
        ]
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader(); writer.writerows(rows)

        df = pd.DataFrame(rows, columns=fieldnames)
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="anunturi")

        if EXPORT_JSONL:
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")

        found = sum(1 for r in rows if r.get("telefon"))
        log.info(f"OK. Salvat {found} numere (unice).")
        log.info(f"CSV : {csv_path}")
        log.info(f"XLSX: {xlsx_path}")
        if EXPORT_JSONL:
            log.info(f"JSONL: {jsonl_path}")
        log.info(f"Anunțuri vizitate: {visited_ads}. Rânduri totale: {len(rows)}.")

    finally:
        pass


if __name__ == "__main__":
    main()
