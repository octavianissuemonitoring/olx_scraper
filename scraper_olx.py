from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import subprocess
import time
import unicodedata

# --- suprimÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ warning-ul "pkg_resources is deprecated as an API" din dependenÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºe ---
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
from dotenv import load_dotenv
from selenium.common.exceptions import ElementClickInterceptedException, ElementNotInteractableException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from seleniumwire import webdriver as wire_webdriver

warnings.filterwarnings("ignore", category=UserWarning, message=r"pkg_resources is deprecated as an API.*")


# ========= LOGGING minimalist =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("olx")
# reduc zgomotul din dependency
logging.getLogger("seleniumwire").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def log_stage(stage: str, status: str, details: str = ""):
    """Helper pentru a marca clar etapele ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n CMD."""
    msg = f"[{stage}] {status}"
    if details:
        msg += f" | {details}"
    log.info(msg)


# ========= CONFIG UÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã¢â‚¬Â¹Ãƒâ€¦Ã¢â‚¬Å“OR =========
HEADLESS = False
OUTPUT_PREFIX = "anunturi_autorulote"
EXPORT_JSONL = True
MAX_PAGES_PER_SEED = None  # pune 1 pentru test rapid; None = fÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ limitÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
MAX_PAGE_RETRIES = 4
MAX_AD_RETRIES = 3
BACKOFF_BASE = 1.0
BACKOFF_FACTOR = 2.0
BACKOFF_JITTER = 0.35
SLEEP_BETWEEN_ADS = 0.9
ASSISTED_LOGIN_TIMEOUT = 90
DEBUG_SNAPSHOTS = True  # pune False dupÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ ce termini depanarea

# login unic doar ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n driverul ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œADÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â
COOKIES_FILE = "olx_cookies.json"

# UA & viewport
VIEWPORT_W = (1200, 1920)
VIEWPORT_H = (740, 1080)
UA_POOL = [
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
        "platform": "Win32",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
        "platform": "Win32",
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
        "platform": "MacIntel",
    },
]

FIXED_AD_UA = {
    "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "lang": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    "platform": "Win32",
}

# regex
PHONE_RE = re.compile(
    r"(?:\+?4?0\s*7[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})" r"|(?:07[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
)
RE_ID = re.compile(r"\bID(?:-ul)?(?:\s*anuntului)?\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
RE_VIEWS = re.compile(r"\bVizualizari\s*[:#]?\s*([\d\s\.]+)", re.IGNORECASE)
# Permite: "Garantie (RON): 5 000", "Garantie: 5 000 RON", "garantie- 5000 Lei", etc.
# Logica: dupÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ cuv. "Garantie", acceptÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ pÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢nÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ la 40 de caractere non-cifrÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ (paranteze/colon/spaÃƒÆ’Ã‹â€ ÃƒÂ¢Ã¢â€šÂ¬Ã‚Âºii),
# apoi CAPTUREAZÃƒÆ’Ã¢â‚¬Å¾ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ un numÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢r care ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â®ncepe cu cel puÃƒÆ’Ã‹â€ ÃƒÂ¢Ã¢â€šÂ¬Ã‚Âºin o cifrÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢, urmatÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ de cifre/spaÃƒÆ’Ã‹â€ ÃƒÂ¢Ã¢â€šÂ¬Ã‚Âºii/puncte,
# ÃƒÆ’Ã‹â€ ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢i opÃƒÆ’Ã‹â€ ÃƒÂ¢Ã¢â€šÂ¬Ã‚Âºional moneda dupÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ (RON/Lei/EUR/ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬).
RE_GARANTIE = re.compile(
    r"\bGarantie\b[^\d]{0,40}(\d[\d\s\.]*)(?:\s*(?:RON|Lei|EUR|ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬))?",
    re.IGNORECASE,
)


# ========= STRUCTURI & CONFIG EXTERN =========
@dataclass
class ProxyEndpoint:
    protocol: str
    host: str
    port: int
    username: str = ""
    password: str = ""


@dataclass
class ProxyPools:
    verify_ssl: bool
    list_endpoints: List[ProxyEndpoint]
    ad_endpoints: List[ProxyEndpoint]


def read_urls(path="urls.txt") -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError("LipseÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢te urls.txt")
    with open(path, "r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    if not urls:
        raise ValueError("urls.txt este gol")
    return urls


def load_proxies(path="proxies.json") -> ProxyPools:
    if not os.path.exists(path):
        return ProxyPools(True, [], [])
    data = json.load(open(path, "r", encoding="utf-8"))
    verify = bool(data.get("verify_ssl", True))

    def mk(key) -> List[ProxyEndpoint]:
        out = []
        for ep in data.get(key, []):
            out.append(
                ProxyEndpoint(
                    protocol=str(ep["protocol"]).strip(),
                    host=str(ep["host"]).strip(),
                    port=int(ep["port"]),
                    username=str(ep.get("username", "")),
                    password=str(ep.get("password", "")),
                )
            )
        return out

    return ProxyPools(verify, mk("list_endpoints"), mk("ad_endpoints"))


def load_secrets(path="secrets.env") -> Tuple[str, str]:
    load_dotenv(path)
    return os.getenv("OLX_EMAIL", "").strip(), os.getenv("OLX_PASSWORD", "").strip()


# ========= HELPERI =========
def pick_ua() -> dict:
    return random.choice(UA_POOL)


def exp_backoff(attempt: int) -> None:
    base = BACKOFF_BASE * (BACKOFF_FACTOR ** (attempt - 1))
    jitter = base * BACKOFF_JITTER
    time.sleep(random.uniform(max(0.05, base - jitter), base + jitter))


def sanitize_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def strip_diacritics(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def parse_price(raw: str) -> Tuple[str, str]:
    if not raw:
        return "", ""
    m = re.search(r"([\d\.\s]+)\s*([ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬EeUuRrOo]|RON|Lei|LEI)?", raw)
    if not m:
        return "", ""
    val = re.sub(r"[^\d]", "", m.group(1) or "").strip()
    cur = (m.group(2) or "").upper().replace("LEI", "RON").replace("EURO", "ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬")
    if cur in ["E", "EURO"]:
        cur = "ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬"
    return val, cur or ""


def parse_total_results(driver) -> Optional[int]:
    """CiteÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢te 'Am gÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢sit X rezultate' din header, dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ existÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢."""
    # cautÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ o frazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ cu 'Am gÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢sit' + 'rezultate'
    try:
        el = driver.find_element(
            By.XPATH,
            "//*[contains(., 'Am gÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢sit') and contains(., 'rezultat')]",
        )
        txt = el.text.replace(".", "")
        m = re.search(r"(\d+)", txt)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def normalize_url(href: str) -> str:
    """NormalizeazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ URL-ul: eliminÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ fragmentul ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i parametri de tracking care dubleazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ linkuri."""
    try:
        href = href.split("#", 1)[0]
        s = urlsplit(href)
        # scoatem parametri evidenÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºi de tracking
        drop = {
            "reason",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_term",
            "utm_content",
        }
        qs = [(k, v) for k, v in parse_qsl(s.query, keep_blank_values=True) if k.lower() not in drop]
        return urlunsplit((s.scheme, s.netloc, s.path, urlencode(qs, doseq=True), ""))
    except Exception:
        return href


def clean_phone(s: str) -> str:
    digits = re.sub(r"\D", "", s or "")
    # +40 7xx xxx xxx -> 07xxxxxxxx
    if digits.startswith("40") and len(digits) == 11 and digits[2] == "7":
        digits = "0" + digits[2:]
    if digits.startswith("7") and len(digits) == 9:
        digits = "0" + digits
    return digits


def _ensure_dir(p: str) -> None:
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass


def debug_dump(ad_driver, url: str, tag: str = "no_phone") -> None:
    """SalveazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ screenshot, HTML ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i ultimele XHR-uri utile ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ntr-un subfolder _debug/."""
    if not DEBUG_SNAPSHOTS:
        return
    ts = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.join("_debug", f"{tag}_{ts}")
    _ensure_dir(base)
    # screenshot
    try:
        ad_driver.save_screenshot(os.path.join(base, "page.png"))
    except Exception:
        pass
    # html
    try:
        html = ad_driver.page_source
        open(os.path.join(base, "page.html"), "w", encoding="utf-8").write(html)
    except Exception:
        pass
    # requests (din selenium-wire)
    try:
        reqs = ad_driver.requests[-120:] if hasattr(ad_driver, "requests") else []
        out = []
        for r in reqs:
            try:
                status = r.response.status_code if r.response else None
                ctype = (r.response.headers.get("Content-Type", "") if r.response else "") or ""
                if not any(k in ctype.lower() for k in ("json", "text", "html")):
                    continue
                body = r.response.body
                if isinstance(body, bytes):
                    try:
                        body = body.decode("utf-8", "ignore")
                    except Exception:
                        continue
                out.append(
                    {
                        "url": r.url,
                        "status": status,
                        "content_type": ctype,
                        "snippet": body[:2000] if body else "",
                    }
                )
            except Exception:
                continue
        open(os.path.join(base, "network.json"), "w", encoding="utf-8").write(
            json.dumps(out, ensure_ascii=False, indent=2)
        )
    except Exception:
        pass
    # mic index
    try:
        open(os.path.join(base, "README.txt"), "w", encoding="utf-8").write(f"URL: {url}\nTimestamp: {ts}\n")
    except Exception:
        pass


def _safe_click(driver, el) -> bool:
    try:
        el.click()
        return True
    except (ElementClickInterceptedException, ElementNotInteractableException):
        pass
    try:
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        pass
    try:
        ActionChains(driver).move_to_element(el).pause(0.1).click().perform()
        return True
    except Exception:
        return False


def _phones_from_dom(driver) -> List[str]:
    phones = set()
    # 1) linkuri tel:
    try:
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href^='tel:']"):
            href = a.get_attribute("href") or ""
            if href.lower().startswith("tel:"):
                ph = clean_phone(href.split(":", 1)[1])
                if ph:
                    phones.add(ph)
    except Exception:
        pass
    # 2) body text
    try:
        body = driver.find_element(By.TAG_NAME, "body").text
        for m in PHONE_RE.findall(body):
            m = m if isinstance(m, str) else next(filter(None, m), "")
            if m:
                ph = clean_phone(m)
                if ph:
                    phones.add(ph)
    except Exception:
        pass
    return [p for p in phones if p.startswith("07") and len(p) == 10]


def _phones_from_network(driver, tail: int = 120) -> List[str]:
    """CautÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n ultimele rÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢spunsuri XHR texte care conÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºin numere (JSON/text)."""
    phones = set()
    try:
        reqs = driver.requests[-tail:] if hasattr(driver, "requests") else []
    except Exception:
        reqs = []
    for req in reversed(reqs):
        try:
            if not req.response:
                continue
            ctype = (req.response.headers.get("Content-Type", "") or "").lower()
            if not any(k in ctype for k in ("json", "text", "html")):
                continue
            body = req.response.body
            if isinstance(body, bytes):
                try:
                    body = body.decode("utf-8", "ignore")
                except Exception:
                    continue
            if not body:
                continue
            for m in PHONE_RE.findall(body):
                m = m if isinstance(m, str) else next(filter(None, m), "")
                if m:
                    ph = clean_phone(m)
                    if ph.startswith("07") and len(ph) == 10:
                        phones.add(ph)
        except Exception:
            continue
    return sorted(phones)


SHOW_PHONE_SELECTORS: List[Tuple[str, str]] = [
    # OLX varianta nouÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
    (By.CSS_SELECTOR, "[data-testid='show-phone-number']"),
    (By.CSS_SELECTOR, "[data-cy='ad-contact-phone']"),
    # butoane generice cu text
    (
        By.XPATH,
        "//button[contains(., 'AratÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢') or contains(., 'Arata') or contains(., 'Show')]",
    ),
    (
        By.XPATH,
        "//*[self::button or self::a][.//span[contains(.,'Telefon')] or contains(.,'Telefon')]",
    ),
    # fallback: linkuri tel:
    (By.CSS_SELECTOR, "a[href^='tel:']"),
]


def build_wire_options(ep: Optional[ProxyEndpoint], verify_ssl: bool) -> Optional[dict]:
    if not ep:
        return None
    auth = f"{ep.username}:{ep.password}@" if (ep.username or ep.password) else ""
    if ep.protocol.lower() == "http":
        proxy = {
            "http": f"http://{auth}{ep.host}:{ep.port}",
            "https": f"https://{auth}{ep.host}:{ep.port}",
        }
    elif ep.protocol.lower() == "socks5":
        proxy = {
            "http": f"socks5://{auth}{ep.host}:{ep.port}",
            "https": f"socks5://{auth}{ep.host}:{ep.port}",
        }
    else:
        raise ValueError(f"Protocol necunoscut: {ep.protocol}")
    out = {"proxy": proxy}
    if not verify_ssl:
        out["verify_ssl"] = False
    return out


def apply_stealth(driver, ua: dict) -> None:
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": f"""
            Object.defineProperty(navigator,'webdriver',{{get:()=>undefined}});
            window.chrome={{runtime:{{}}}};
            Object.defineProperty(navigator,'languages',{{get:()=>['ro-RO','ro','en-US','en']}});
            Object.defineProperty(navigator,'plugins',{{get:()=>[1,2,3,4,5]}});
            Object.defineProperty(navigator,'platform',{{get:()=>'{ua.get('platform', 'Win32')}' }});
        """
        },
    )


def make_driver(ep: Optional[ProxyEndpoint], verify_ssl: bool, ua: Optional[dict] = None):
    if ua is None:
        ua = pick_ua()
    w, h = random.randint(*VIEWPORT_W), random.randint(*VIEWPORT_H)
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument(f"--window-size={w},{h}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--lang={ua['lang']}")
    opts.add_argument(f"--user-agent={ua['ua']}")
    # suprimÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢m logurile ChromeDriver/DevTools
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_argument("--log-level=3")
    service = Service(log_output=subprocess.DEVNULL)

    # selenium-wire options + decomprimare body XHR (disable_encoding)
    sw = build_wire_options(ep, verify_ssl) if ep else {}
    if sw is None:
        sw = {}
    sw["disable_encoding"] = (
        True  # important pentru a citi uÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢or response.body
    )

    d = wire_webdriver.Chrome(seleniumwire_options=sw, options=opts, service=service)
    apply_stealth(d, ua)
    return d


# ========= UI helpers =========
def accept_cookies_if_any(driver) -> None:
    cands = [
        (By.CSS_SELECTOR, "[data-testid='cookies-popup-accept-all']"),
        (
            By.XPATH,
            "//button[contains(., 'AcceptÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ toate') or contains(., 'Accepta toate') or contains(., 'Accept all')]",
        ),
    ]
    for by, sel in cands:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
            time.sleep(0.2)
            return
        except Exception:
            pass


def is_logged_in(driver) -> bool:
    try:
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-user-name']"):
            return True
        if driver.find_elements(By.CSS_SELECTOR, "[data-testid='user-profile-link']"):
            return True
        if driver.find_elements(By.XPATH, "//a[contains(., 'Contul meu') or contains(., 'Account')]"):
            return True
    except Exception:
        pass
    return False


# ========= COOKIES persistente (un singur login) =========
def save_cookies(driver, path=COOKIES_FILE) -> None:
    try:
        cookies = driver.get_cookies()
        json.dump(cookies, open(path, "w", encoding="utf-8"))
        log_stage("LOGIN", "INFO", f"cookies salvate ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n {path}")
    except Exception as e:
        log_stage("LOGIN", "INFO", f"nu am putut salva cookies: {e}")


def load_cookies(driver, base="https://www.olx.ro/", path=COOKIES_FILE) -> bool:
    if not os.path.exists(path):
        return False
    try:
        driver.get(base)
        for ck in json.load(open(path, "r", encoding="utf-8")):
            try:
                driver.add_cookie(ck)
            except Exception:
                pass
        driver.get(base)
        return True
    except Exception as e:
        log_stage(
            "LOGIN",
            "INFO",
            f"nu am putut ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rca cookies: {e}",
        )
        return False


def ensure_single_login(ad_driver, email: str, password: str) -> None:
    log_stage("LOGIN", "STARTING")
    # 1) ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncearcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ din cookies
    if load_cookies(ad_driver):
        if is_logged_in(ad_driver):
            log_stage("LOGIN", "END OK", "autologin din cookies")
            return
    # 2) eÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ti deja logat?
    ad_driver.get("https://www.olx.ro/")
    accept_cookies_if_any(ad_driver)
    if is_logged_in(ad_driver):
        save_cookies(ad_driver)
        log_stage("LOGIN", "END OK", "deja logat")
        return

    # 3) login automat cu email/parolÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
    if email and password:
        try:
            ad_driver.get("https://www.olx.ro/cont/")
            accept_cookies_if_any(ad_driver)
            time.sleep(1)
            email_sel = [
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[name='email']"),
                (By.CSS_SELECTOR, "input[name='username']"),
                (
                    By.XPATH,
                    "//input[contains(@placeholder,'Email') or contains(@placeholder,'Telefon')]",
                ),
            ]
            pwd_sel = [
                (By.CSS_SELECTOR, "input[type='password']"),
                (By.CSS_SELECTOR, "input[name='password']"),
                (
                    By.XPATH,
                    "//input[contains(@placeholder,'Parol') or contains(@placeholder,'password')]",
                ),
            ]
            email_box = None
            for by, sel in email_sel:
                try:
                    email_box = WebDriverWait(ad_driver, 6).until(EC.presence_of_element_located((by, sel)))
                    break
                except Exception:
                    pass
            if email_box:
                email_box.clear()
                email_box.send_keys(email)
                time.sleep(0.2)
            pwd_box = None
            for by, sel in pwd_sel:
                try:
                    pwd_box = ad_driver.find_element(by, sel)
                    break
                except Exception:
                    pass
            if pwd_box:
                pwd_box.clear()
                pwd_box.send_keys(password)
                time.sleep(0.2)
                pwd_box.send_keys(Keys.ENTER)

            for _ in range(20):
                if is_logged_in(ad_driver):
                    save_cookies(ad_driver)
                    log_stage("LOGIN", "END OK", "auto")
                    return
                time.sleep(1)
        except Exception:
            pass

    # 4) login asistat (o singurÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ datÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢)
    ad_driver.get("https://www.olx.ro/cont/")
    accept_cookies_if_any(ad_driver)
    log_stage(
        "LOGIN",
        "EXECUTING",
        f"manual, ai ~{ASSISTED_LOGIN_TIMEOUT}s ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n fereastrÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢",
    )
    t0 = time.time()
    while time.time() - t0 < ASSISTED_LOGIN_TIMEOUT:
        if is_logged_in(ad_driver):
            save_cookies(ad_driver)
            log_stage("LOGIN", "END OK", "manual")
            return
        time.sleep(1.5)
    log_stage("LOGIN", "END FAIL", "nu s-a finalizat autentificarea")


# ========= LISTÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ & ANUNÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ =========
def wait_for_list(driver) -> None:
    try:
        WebDriverWait(driver, 12).until(
            EC.any_of(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-cy='l-card']")),
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(., 'Nu am gÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢sit anunÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºuri') or contains(., 'No results')]",
                    )
                ),
            )
        )
    except Exception:
        pass


def collect_links(driver) -> Tuple[List[Tuple[str, str]], Dict[str, int]]:
    """
    ReturneazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢: (links_olx, stats)
      links_olx: listÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ de (title, href_normalizat) DOAR pentru /d/oferta/
      stats: contorizÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ri pentru ce-am ignorat (autovit, intern-other)
    """
    stats = {"olx": 0, "autovit": 0, "other_internal": 0}
    out: List[Tuple[str, str]] = []
    seen = set()

    # deruleazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ un pic pentru a forÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºa ataÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢area lazy (de cÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢teva ori)
    try:
        for _ in range(5):
            driver.execute_script("window.scrollBy(0, Math.floor(document.body.scrollHeight/5));")
            time.sleep(0.15)
    except Exception:
        pass
    time.sleep(0.2)

    # 1) ia toate cardurile cunoscute; 2) fallback: toate ancorele din paginÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
    anchors = []
    cards = driver.find_elements(By.CSS_SELECTOR, "[data-cy='l-card'], article")
    for c in cards:
        try:
            anchors.extend(c.find_elements(By.CSS_SELECTOR, "a[href]"))
        except Exception:
            pass
    if not anchors:
        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")

    for a in anchors:
        href = a.get_attribute("href") or ""
        if not href:
            continue
        txt = (a.text or "").strip()

        if "/d/oferta/" in href:
            url = normalize_url(href)
            if url not in seen:
                seen.add(url)
                out.append((txt, url))
                stats["olx"] += 1
        elif "autovit.ro" in href:
            stats["autovit"] += 1
        elif href.startswith("https://www.olx.ro"):
            # alte pagini interne (ex. /store/, /profile/, /help/). Le contorizÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢m pentru diagnozÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢.
            stats["other_internal"] += 1

    return out, stats


def first_text(driver, sels: List[Tuple[str, str]]) -> str:
    for by, sel in sels:
        try:
            el = driver.find_element(by, sel)
            t = (el.text or "").strip()
            if t:
                return t
        except Exception:
            pass
    return ""


def extract_fields(driver) -> Dict[str, str]:
    titlu = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-cy='offer_title'] h1, [data-cy='offer_title'] h4"),
            (By.CSS_SELECTOR, "h1[data-cy='ad_title']"),
            (
                By.CSS_SELECTOR,
                "[data-testid='offer_title'] h1, [data-testid='offer_title'] h4",
            ),
        ],
    )
    pret = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-testid='ad-price-container']"),
            (
                By.XPATH,
                "//*[self::h3 or self::h2][contains(., 'ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬') or contains(., 'Lei') or contains(., 'RON')]",
            ),
        ],
    )
    persoana = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-testid='user-type']"),
            (
                By.XPATH,
                "//p[contains(., 'Persoana') or contains(., 'PersoanÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢') or contains(., 'Firm')]",
            ),
            (By.CSS_SELECTOR, "p.css-5l1a1j span"),
        ],
    )
    vanzator = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-testid='user-profile-user-name']"),
            (By.CSS_SELECTOR, "[data-testid='user-profile-link']"),
            (By.XPATH, "//h4[contains(@data-testid,'user-profile-user-name')]"),
        ],
    )
    descriere = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-testid='ad_description']"),
            (By.CSS_SELECTOR, "[data-cy='ad_description']"),
        ],
    )
    descriere = sanitize_text(descriere)
    pv, pc = parse_price(pret)

    try:
        body = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body = ""
    norm = strip_diacritics(body)

    garantie = ""
    m = RE_GARANTIE.search(norm)
    if m:
        garantie = sanitize_text(m.group(1))
    id_anunt = ""
    m = RE_ID.search(norm)
    if m:
        id_anunt = sanitize_text(m.group(1))
    viz = ""
    m = RE_VIEWS.search(norm)
    if m:
        viz = sanitize_text(m.group(1))

    return {
        "titlu": titlu,
        "pret": pret,
        "pret_valoare": pv,
        "pret_moneda": pc,
        "persoana": persoana,
        "garantie": garantie,
        "descriere": descriere,
        "id_anunt": id_anunt,
        "vizualizari": viz,
        "vanzator": vanzator,
    }


def reveal_phone_robust(driver) -> List[str]:
    """
    Strategie ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n lanÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âº:
    1) scan DOM (inclusiv <a href="tel:">)
    2) ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncearcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ toate butoanele ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¾AratÃƒÆ’Ã¢â‚¬Å¾Ãƒâ€ Ã¢â‚¬â„¢ telefonulÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â cu 3 tipuri de click
    3) aÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢teaptÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ apariÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºia numÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rului ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n DOM
    4) cautÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n XHR (selenium-wire) rÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢spunsul care conÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºine numÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rul
    5) fallback: deschide varianta mobilÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ntr-un tab nou ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i citeÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢te <a href="tel:">
    """
    # 0) pre-scan DOM
    nums = _phones_from_dom(driver)
    if nums:
        return sorted(set(nums))

    # 1) ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncearcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ de pÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢nÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ la 3 ori click pe diverse butoane
    for round_idx in range(3):
        try:
            driver.execute_script("window.scrollBy(0, 350);")
        except Exception:
            pass
        time.sleep(0.3)

        clicked = False
        for by, sel in SHOW_PHONE_SELECTORS:
            try:
                candidates = driver.find_elements(by, sel)
                for btn in candidates:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                    time.sleep(0.15)
                    if _safe_click(driver, btn):
                        clicked = True
                        time.sleep(1.5)  # dÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ timp UI-ului/ XHR-ului
                        # dupÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ fiecare click, verificÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ rapid DOM
                        nums = _phones_from_dom(driver)
                        if nums:
                            return sorted(set(nums))
            except Exception:
                continue

        # 2) dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ am fÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢cut clic, dar ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ nu vedem numÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rul, ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncearcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ din XHR
        if clicked:
            nums = _phones_from_network(driver, tail=160)
            if nums:
                return sorted(set(nums))

        # 3) ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n caz cÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ apare din nou bannerul de cookies
        accept_cookies_if_any(driver)

    # 4) fallback: ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncearcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ varianta mobilÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ntr-un tab separat
    try:
        cur = driver.current_url
        mobile_url = re.sub(r"^https?://(?:www\.)?olx\.ro", "https://m.olx.ro", cur, flags=re.I)
        if mobile_url != cur:
            driver.execute_script("window.open(arguments[0],'_blank');", mobile_url)
            driver.switch_to.window(driver.window_handles[-1])
            WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(0.8)
            accept_cookies_if_any(driver)
            nums = _phones_from_dom(driver)
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            if nums:
                return sorted(set(nums))
    except Exception:
        try:
            if len(driver.window_handles) > 0:
                driver.switch_to.window(driver.window_handles[0])
        except Exception:
            pass

    return []


# ========= RUNNERS (LIST fÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ login, AD cu login unic) =========
def try_list_page(list_driver, url: str) -> List[Tuple[str, str]]:
    log_stage("LIST_PAGE", "STARTING", f"url={url}")
    try:
        list_driver.get(url)
        accept_cookies_if_any(list_driver)
        wait_for_list(list_driver)

        total = parse_total_results(list_driver)
        links, stats = collect_links(list_driver)

        msg = f"links={len(links)}"
        if total is not None:
            msg += f" | total={total}"
        # arÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢tÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢m ce-am sÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rit: autovit ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i alte interne
        msg += f" | skipped autovit={stats.get('autovit', 0)}, other={stats.get('other_internal', 0)}"

        log_stage("LIST_PAGE", "END OK", msg)
        return links
    except Exception as e:
        log_stage("LIST_PAGE", "END FAIL", str(e))
        return []


def try_ad_page(ad_driver, href: str) -> Tuple[Dict[str, str], List[str]]:
    log_stage("AD", "STARTING", f"url={href}")
    try:
        ad_driver.get(href)
        WebDriverWait(ad_driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.4)
        accept_cookies_if_any(ad_driver)

        fields = extract_fields(ad_driver)
        phones = reveal_phone_robust(ad_driver)
        if not phones:
            debug_dump(ad_driver, href, tag="no_phone")

        log_stage("AD", "END OK", f"phones={len(phones)}")
        return fields, phones

    except Exception as e:
        log_stage("AD", "END FAIL", str(e))
        try:
            debug_dump(ad_driver, href, tag="ad_fail")
        except Exception:
            pass
        empty = {
            "titlu": "",
            "pret": "",
            "pret_valoare": "",
            "pret_moneda": "",
            "persoana": "",
            "garantie": "",
            "descriere": "",
            "id_anunt": "",
            "vizualizari": "",
            "vanzator": "",
        }
        return empty, []


# ========= MAIN =========
def main():
    # fiÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢iere externe
    seeds = read_urls("urls.txt")
    pools = load_proxies("proxies.json")
    email, password = load_secrets("secrets.env")

    # 1) ad_driver: STICKY RO (primul din ad_endpoints) ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â login unic
    ad_ep = pools.ad_endpoints[0] if pools.ad_endpoints else None
    ad_driver = make_driver(ad_ep, pools.verify_ssl, ua=FIXED_AD_UA)
    ensure_single_login(
        ad_driver, email, password
    )  # o singurÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ datÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢

    # 2) list_driver: ROTATING (primul din list_endpoints). DacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ gateway-ul e rotating, ajunge 1.
    list_ep = pools.list_endpoints[0] if pools.list_endpoints else None
    list_driver = make_driver(list_ep, pools.verify_ssl)

    rows: List[Dict[str, str]] = []
    seen = set()
    visited = 0

    try:
        for seed in seeds:
            page_idx = 1
            while True:
                if MAX_PAGES_PER_SEED and page_idx > MAX_PAGES_PER_SEED:
                    break

                url = seed if page_idx == 1 else f"{seed.rstrip('/')}/?page={page_idx}"

                # === LIST PAGE (fÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ login) ===
                links: List[Tuple[str, str]] = []
                for attempt in range(1, MAX_PAGE_RETRIES + 1):
                    log_stage(
                        "LIST_PAGE",
                        "EXECUTING",
                        f"attempt={attempt}/{MAX_PAGE_RETRIES} | url={url}",
                    )
                    links = try_list_page(list_driver, url)
                    if links:
                        break
                    exp_backoff(attempt)

                if not links:
                    # nimic pe pagina curentÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ -> trecem la urmÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢torul seed
                    break

                # === AD PAGES (cu driver autenticat, cookies persistente) ===
                for _, href in links:
                    fields: Dict[str, str]
                    phones: List[str]
                    for attempt in range(1, MAX_AD_RETRIES + 1):
                        log_stage("AD", "EXECUTING", f"attempt={attempt}/{MAX_AD_RETRIES}")
                        fields, phones = try_ad_page(ad_driver, href)
                        if fields.get("titlu") or phones:
                            break
                        # dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ s-a dereglat driverul sticky, ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®l refacem ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢m cookies
                        try:
                            ad_driver.quit()
                        except Exception:
                            pass
                        ad_driver = make_driver(ad_ep, pools.verify_ssl, ua=FIXED_AD_UA)
                        # reÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢m cookies ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i verificÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢m cÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ suntem logaÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºi
                        load_cookies(ad_driver)
                        if not is_logged_in(ad_driver):
                            ensure_single_login(ad_driver, email, password)
                        exp_backoff(attempt)

                    visited += 1

                    if phones:
                        for ph in phones:
                            key = (ph, href)
                            if key in seen:
                                continue
                            seen.add(key)
                            rows.append({"telefon": ph, **fields, "url": href})
                    else:
                        key = ("", href)
                        if key in seen:
                            continue
                        seen.add(key)
                        rows.append({"telefon": "", **fields, "url": href})

                    time.sleep(SLEEP_BETWEEN_ADS)

                page_idx += 1

        # === EXPORT ===
        log_stage("EXPORT", "STARTING")
        ts = time.strftime("%Y%m%d-%H%M%S")
        csv_path = f"{OUTPUT_PREFIX}_{ts}.csv"
        xlsx_path = f"{OUTPUT_PREFIX}_{ts}.xlsx"
        jsonl_path = f"{OUTPUT_PREFIX}_{ts}.jsonl"

        cols = [
            "telefon",
            "titlu",
            "pret",
            "pret_valoare",
            "pret_moneda",
            "persoana",
            "garantie",
            "descriere",
            "id_anunt",
            "vizualizari",
            "vanzator",
            "url",
        ]

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)

        pd.DataFrame(rows, columns=cols).to_excel(xlsx_path, index=False)

        if EXPORT_JSONL:
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")

        found = sum(1 for r in rows if r.get("telefon"))
        log_stage(
            "EXPORT",
            "END OK",
            f"found={found} | ads_visited={visited} | CSV={csv_path}",
        )
        if EXPORT_JSONL:
            log.info(f"JSONL: {jsonl_path}")
        log.info(f"XLSX : {xlsx_path}")

    finally:
        # ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®nchidem driverele indiferent ce s-a ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ntÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢mplat
        try:
            list_driver.quit()
        except Exception:
            pass
        try:
            ad_driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
