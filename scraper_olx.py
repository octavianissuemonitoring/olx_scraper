from uuid import uuid4

"""
OLX scraper (Selenium 4, fÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢rÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ selenium-wire)
- Login cu salvare/ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢rcare cookie-uri (o singurÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ autentificare per sesiune)
- Progres clar [curent/total]
- Extragere cÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢mpuri suplimentare: ad_id, user_id, locality (prin JSONÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¹Ãƒâ€¦Ã¢â‚¬Å“LD ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ meta ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ DOM fallback)
- Salvare imediatÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ dupÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ FIECARE paginÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢: CSV (ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢i opÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºional JSONL), cu flush
- Compatibil cu structura ta (seed-uri din urls.txt, proxies.json, secrets.env)

Rulare (PowerShell):

  $env:OLX_EMAIL = "emailul_tau@exemplu.ro"
  $env:OLX_PASSWORD = "parola_ta"
  python .\\scraper_olx.py

DependenÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºe: selenium>=4.20, beautifulsoup4, pandas, python-dotenv, tqdm (opÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºional)
  python -m pip install --upgrade "selenium>=4.20" beautifulsoup4 pandas python-dotenv tqdm
"""
import csv
import json
import logging
import os
import platform
import random
import re
import socket
import subprocess
import sys
import time
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import WebDriverException  # <-- nou
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    InvalidSessionIdException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from tqdm import tqdm
except Exception:

    def tqdm(iterable, total=None):
        return iterable


# ========= LOGGING =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("olx")


def log_stage(stage: str, status: str, details: str = ""):
    msg = f"[{stage}] {status}"
    if details:
        msg += f" | {details}"
    log.info(msg)


# ========= RUN LOGGING (run-id, files per run + all.log) =========

RUN_ID = None
RUN_START_TS = None
RUN_LOG_PATH = None


def init_run_logging():
    """ConfigureazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ logging-ul: consolÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ + logs/all.log (zilnic) + logs/runs/run-<RUN_ID>.log"""
    global RUN_ID, RUN_START_TS, RUN_LOG_PATH, log
    RUN_START_TS = time.time()
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    RUN_ID = f"{ts}-{str(uuid4())[:8]}"

    logs_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(os.path.join(logs_dir, "runs"), exist_ok=True)

    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | run=%(run_id)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

    th = TimedRotatingFileHandler(
        filename=os.path.join(logs_dir, "all.log"),
        when="midnight",
        backupCount=int(os.getenv("LOG_KEEP_DAYS", "60")),
        encoding="utf-8",
        utc=True,
        delay=False,
    )
    th.setLevel(level)
    th.setFormatter(fmt)
    root.addHandler(th)

    RUN_LOG_PATH = os.path.join(logs_dir, "runs", f"run-{RUN_ID}.log")
    rh = RotatingFileHandler(
        filename=RUN_LOG_PATH,
        maxBytes=int(float(os.getenv("RUN_LOG_MAX_MB", "10")) * 1024 * 1024),
        backupCount=int(os.getenv("RUN_LOG_BACKUPS", "3")),
        encoding="utf-8",
        delay=False,
    )
    rh.setLevel(level)
    rh.setFormatter(fmt)
    root.addHandler(rh)

    logger = logging.getLogger("olx")
    log = logging.LoggerAdapter(logger, {"run_id": RUN_ID})


def finalize_run_index(extra: dict | None = None):
    """Scrie indexul JSONL cu sumarul rularii curente."""
    try:
        if RUN_ID is None or RUN_START_TS is None:
            return
        end = time.time()
        summary = {
            "run_id": RUN_ID,
            "start_ts": RUN_START_TS,
            "end_ts": end,
            "duration_s": round(end - RUN_START_TS, 3),
            "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end)),
            "output_prefix": OUTPUT_PREFIX,
            "headless": bool(HEADLESS),
            "host": socket.gethostname(),
            "python": sys.version.split()[0],
            "os": platform.platform(),
            "run_log": RUN_LOG_PATH,
        }
        if isinstance(extra, dict):
            summary.update(extra)
        idx_path = os.path.join(os.getenv("LOG_DIR", "logs"), "runs", "index.jsonl")
        os.makedirs(os.path.dirname(idx_path), exist_ok=True)
        with open(idx_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(summary, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ========= CONFIG =========
HEADLESS = True
OUTPUT_PREFIX = "anunturi_autorulote"
EXPORT_JSONL = True
MAX_PAGES_PER_SEED = None  # pune 1 pentru test; None = fÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢rÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ limitÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢
MAX_PAGE_RETRIES = 4
MAX_AD_RETRIES = 3
BACKOFF_BASE = 1.0
BACKOFF_FACTOR = 2.0
BACKOFF_JITTER = 0.35
SLEEP_BETWEEN_ADS = 0.9
JITTER = (
    0.6,
    1.3,
)  # pauzÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ aleatoare ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ntre anunÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºuri (secunde)
ASSISTED_LOGIN_TIMEOUT = 90
DEBUG_SNAPSHOTS = False
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

# ========= RUN STATS (contorizÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ri pentru index) =========
STATS = {
    "links_total": 0,
    "seen_skip": 0,
    "ads_attempted": 0,
    "ads_saved": 0,
    "phones_found": 0,
    "errors": 0,
}


def stat_inc(key: str, delta: int = 1) -> None:
    try:
        STATS[key] = STATS.get(key, 0) + int(delta)
    except Exception:
        pass


PHONE_RE = re.compile(
    r"(?:\+?4?0\s*7[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})|(?:07[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
)
RE_ID = re.compile(r"\bID(?:-ul)?(?:\s*anuntului)?\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
RE_VIEWS = re.compile(r"\bVizualizari\s*[:#]?\s*([\d\s\.]+)", re.IGNORECASE)
RE_GARANTIE = re.compile(
    r"\bGarantie\b[^\d]{0,40}(\d[\d\s\.]*)(?:\s*(?:RON|Lei|EUR|ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬))?",
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
        raise FileNotFoundError(
            "LipseÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢te urls.txt"
        )
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
    m = re.search(
        r"([\d\.\s]+)\s*(RON|Lei|LEI|EUR|ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬)?",
        raw,
        re.IGNORECASE,
    )
    if not m:
        return "", ""
    val = re.sub(r"[^\d]", "", m.group(1) or "").strip()
    cur = (
        (m.group(2) or "")
        .upper()
        .replace("LEI", "RON")
        .replace("EURO", "EUR")
        .replace(
            "ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬",
            "EUR",
        )
    )
    return val, cur or ""


def parse_total_results(driver) -> Optional[int]:
    try:
        el = driver.find_element(
            By.XPATH,
            "//*[contains(., 'Am gÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢sit') and contains(., 'rezultat')]",
        )
        txt = el.text.replace(".", "")
        m = re.search(r"(\d+)", txt)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def normalize_url(href: str) -> str:
    try:
        href = href.split("#", 1)[0]
        s = urlsplit(href)
        drop = {"reason", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}
        qs = [(k, v) for k, v in parse_qsl(s.query, keep_blank_values=True) if k.lower() not in drop]
        return urlunsplit((s.scheme, s.netloc, s.path, urlencode(qs, doseq=True), ""))
    except Exception:
        return href


def clean_phone(s: str) -> str:
    digits = re.sub(r"\D", "", s or "")
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
    if not DEBUG_SNAPSHOTS:
        return
    ts = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.join("_debug", f"{tag}_{ts}")
    _ensure_dir(base)
    try:
        ad_driver.save_screenshot(os.path.join(base, "page.png"))
    except Exception:
        pass
    try:
        html = ad_driver.page_source
        open(os.path.join(base, "page.html"), "w", encoding="utf-8").write(html)
    except Exception:
        pass
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


# ========= SELENIUM fÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢rÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ selenium-wire =========


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
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_argument("--log-level=3")

    # forÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚ÂºeazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ conexiune directÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ (ignorÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ proxy-uri OS/PAC)
    opts.add_argument("--proxy-server=direct://")
    opts.add_argument("--proxy-bypass-list=*")

    # proxy upstream (dacÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ e definit)
    if ep:
        auth = f"{ep.username}:{ep.password}@" if (ep.username or ep.password) else ""
        if ep.protocol.lower() == "http":
            proxy_arg = f"http://{auth}{ep.host}:{ep.port}"
        elif ep.protocol.lower() == "socks5":
            proxy_arg = f"socks5://{auth}{ep.host}:{ep.port}"
        else:
            raise ValueError(f"Protocol necunoscut: {ep.protocol}")
        opts.add_argument(f"--proxy-server={proxy_arg}")
        # notÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢: verify_ssl nu se aplicÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ direct ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n Chrome; ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®l lÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢sÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢m pentru viitor

    service = Service(log_output=subprocess.DEVNULL)
    d = webdriver.Chrome(options=opts, service=service)
    d.set_page_load_timeout(60)
    d.set_script_timeout(60)
    d.implicitly_wait(2)
    apply_stealth(d, ua)
    return d


# ========= UI helpers =========


def accept_cookies_if_any(driver) -> None:
    cands = [
        (By.CSS_SELECTOR, "[data-testid='cookies-popup-accept-all']"),
        (
            By.XPATH,
            "//button[contains(., 'AcceptÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ toate') or contains(., 'Accepta toate') or contains(., 'Accept all')]",
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


# ========= COOKIES persistente =========


def save_cookies(driver, path=COOKIES_FILE) -> None:
    try:
        cookies = driver.get_cookies()
        json.dump(cookies, open(path, "w", encoding="utf-8"))
        log_stage(
            "LOGIN",
            "INFO",
            f"cookies salvate ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n {path}",
        )
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
            f"nu am putut ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢rca cookies: {e}",
        )
        return False


def ensure_single_login(ad_driver, email: str, password: str) -> webdriver.Chrome:

    def _rebuild():
        log_stage(
            "LOGIN", "INFO", "recreez driver (sesiune invalidÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢)"
        )
        try:
            ad_driver.quit()
        except Exception:
            pass
        # refacem driverul fÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢rÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ proxy (direct), cu UA fix pentru anunÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºuri
        return make_driver(ep=None, verify_ssl=True, ua=FIXED_AD_UA)

    # 1) ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ncearcÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ autologin din cookies
    try:
        if load_cookies(ad_driver) and is_logged_in(ad_driver):
            log_stage("LOGIN", "END OK", "autologin din cookies")
            return ad_driver
    except InvalidSessionIdException:
        ad_driver = _rebuild()

    # 2) mergi la homepage
    try:
        ad_driver.get("https://www.olx.ro/")
    except InvalidSessionIdException:
        ad_driver = _rebuild()
        ad_driver.get("https://www.olx.ro/")

    accept_cookies_if_any(ad_driver)
    if is_logged_in(ad_driver):
        save_cookies(ad_driver)
        log_stage("LOGIN", "END OK", "deja logat")
        return ad_driver

    # 3) login automat cu user/pass (dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ sunt setate)
    if email and password:
        try:
            try:
                ad_driver.get("https://www.olx.ro/cont/")
            except InvalidSessionIdException:
                ad_driver = _rebuild()
                ad_driver.get("https://www.olx.ro/cont/")

            accept_cookies_if_any(ad_driver)
            WebDriverWait(ad_driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input[type='email'],input[name='email'],input[name='username']")
                )
            ).send_keys(email)

            ad_driver.find_element(By.CSS_SELECTOR, "button[type='submit'],button[data-testid*='next']").click()
            WebDriverWait(ad_driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password'],input[name='password']"))
            ).send_keys(password)
            ad_driver.find_element(By.CSS_SELECTOR, "button[type='submit'],button[data-testid*='login']").click()

            WebDriverWait(ad_driver, 30).until(lambda d: is_logged_in(d))
            save_cookies(ad_driver)
            log_stage("LOGIN", "END OK", "auto")
            return ad_driver
        except InvalidSessionIdException:
            ad_driver = _rebuild()
        except Exception:
            pass

    # 4) login asistat (manual)
    try:
        ad_driver.get("https://www.olx.ro/cont/")
    except InvalidSessionIdException:
        ad_driver = _rebuild()
        ad_driver.get("https://www.olx.ro/cont/")

    accept_cookies_if_any(ad_driver)
    log_stage(
        "LOGIN",
        "EXECUTING",
        f"manual, ai ~{ASSISTED_LOGIN_TIMEOUT}s ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n fereastrÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢",
    )
    t0 = time.time()
    while time.time() - t0 < ASSISTED_LOGIN_TIMEOUT:
        try:
            if is_logged_in(ad_driver):
                save_cookies(ad_driver)
                log_stage("LOGIN", "END OK", "manual")
                return ad_driver
        except InvalidSessionIdException:
            ad_driver = _rebuild()
            try:
                ad_driver.get("https://www.olx.ro/cont/")
            except InvalidSessionIdException:
                ad_driver = _rebuild()
        time.sleep(1.5)

    log_stage("LOGIN", "END FAIL", "nu s-a finalizat autentificarea")
    return ad_driver


# ========= LISTÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ & ANUNÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ =========


def wait_for_list(driver) -> None:
    try:
        WebDriverWait(driver, 12).until(
            EC.any_of(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-cy='l-card']")),
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(., 'Nu am gÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢sit anunÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºuri') or contains(., 'No results') ]",
                    )
                ),
            )
        )
    except Exception:
        pass


def collect_links(driver) -> Tuple[List[Tuple[str, str]], Dict[str, int]]:
    stats = {"olx": 0, "autovit": 0, "other_internal": 0}
    out: List[Tuple[str, str]] = []
    seen = set()
    try:
        for _ in range(5):
            driver.execute_script("window.scrollBy(0, Math.floor(document.body.scrollHeight/5));")
            time.sleep(0.15)
    except Exception:
        pass
    time.sleep(0.2)
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


# --- Identificatori suplimentari (ad_id, user_id, locality) ---


def extract_identifiers_from_html(html: str, page_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    ad_id = user_id = locality = None

    # 1) JSON-LD
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        data = None
        try:
            data = json.loads(tag.string or tag.text or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for obj in items:
            if not isinstance(obj, dict):
                continue
            ad_id = ad_id or obj.get("sku")
            ident = obj.get("identifier")
            if not ad_id and isinstance(ident, dict):
                ad_id = ident.get("value") or ident.get("@id") or ident.get("id")
            if not ad_id and isinstance(ident, list):
                for it in ident:
                    if isinstance(it, dict):
                        cand = it.get("value") or it.get("@id") or it.get("id")
                        if isinstance(cand, str) and cand.strip():
                            ad_id = cand
                            break
            if not ad_id:
                for key in ("@id", "id", "url"):
                    v = obj.get(key)
                    if isinstance(v, str):
                        m = re.search(r"(\d{5,})", v)
                        if m:
                            ad_id = m.group(1)
                            break
            for who in ("seller", "author", "publisher"):
                w = obj.get(who)
                if isinstance(w, dict):
                    cand = w.get("@id") or w.get("id") or w.get("identifier")
                    if isinstance(cand, dict):
                        cand = cand.get("value") or cand.get("@id") or cand.get("id")
                    if isinstance(cand, str):
                        m = re.search(r"(\d{4,})", cand)
                        user_id = m.group(1) if m else cand
                elif isinstance(w, list):
                    for ww in w:
                        if isinstance(ww, dict):
                            cand = ww.get("@id") or ww.get("id") or ww.get("identifier")
                            if isinstance(cand, dict):
                                cand = cand.get("value") or cand.get("@id") or cand.get("id")
                            if isinstance(cand, str) and cand.strip():
                                m = re.search(r"(\d{4,})", cand)
                                user_id = m.group(1) if m else cand
                                break
                if user_id:
                    break
            addr = obj.get("address")
            if isinstance(addr, dict):
                locality = locality or addr.get("addressLocality") or addr.get("addressRegion")
            elif isinstance(addr, str):
                locality = locality or addr
    # 2) meta/URL
    if not ad_id:
        for name in ("product:retailer_item_id", "al:android:url", "al:ios:url", "og:url", "twitter:url"):
            tag = soup.find("meta", {"property": name}) or soup.find("meta", {"name": name})
            if tag and tag.get("content"):
                m = re.search(r"(\d{5,})", tag["content"]) or re.search(r"ID[\w-]+", tag["content"], re.I)
                if m:
                    ad_id = m.group(0)
    if not ad_id:
        m = re.search(r"(\d{5,})", page_url) or re.search(r"ID[\w-]+", page_url, re.I)
        if m:
            ad_id = m.group(0)
    # user_id din profil
    if not user_id:
        a = soup.select_one("a[data-testid='user-profile-link'][href]")
        if a:
            href = a.get("href", "")
            m = re.search(r"user(?:id)?=([\w-]+)", href, re.I)
            if m:
                user_id = m.group(1)
            else:
                segs = [s for s in href.split("/") if s]
                if segs:
                    user_id = segs[-1]
    # locality DOM
    if not locality:
        cand = soup.select_one("[data-testid='location']") or soup.select_one("[data-testid='location-text']")
        if cand:
            locality = cand.get_text(strip=True)
    if locality and "," in locality:
        locality = locality.split(",")[0].strip()
    return (ad_id or None), (user_id or None), (locality or None)


# --- cÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢mpuri paginÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ anunÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âº ---


def extract_fields(driver) -> Dict[str, str]:
    titlu = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-cy='offer_title'] h1, [data-cy='offer_title'] h4"),
            (By.CSS_SELECTOR, "h1[data-cy='ad_title']"),
            (By.CSS_SELECTOR, "[data-testid='offer_title'] h1, [data-testid='offer_title'] h4"),
        ],
    )
    pret = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-testid='ad-price-container']"),
            (
                By.XPATH,
                "//*[self::h3 or self::h2][contains(., 'ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬') or contains(., 'Lei') or contains(., 'RON')]",
            ),
        ],
    )
    persoana = first_text(
        driver,
        [
            (By.CSS_SELECTOR, "[data-testid='user-type']"),
            (
                By.XPATH,
                "//p[contains(., 'Persoana') or contains(., 'PersoanÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢') or contains(., 'Firm')]",
            ),
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

    garantie = sanitize_text(RE_GARANTIE.search(norm).group(1)) if RE_GARANTIE.search(norm) else ""
    id_anunt_text = sanitize_text(RE_ID.search(norm).group(1)) if RE_ID.search(norm) else ""
    viz = sanitize_text(RE_VIEWS.search(norm).group(1)) if RE_VIEWS.search(norm) else ""

    ad_id, user_id, locality = extract_identifiers_from_html(driver.page_source, driver.current_url)
    id_final = ad_id or id_anunt_text

    return {
        "titlu": titlu,
        "pret": pret,
        "pret_valoare": pv,
        "pret_moneda": pc,
        "persoana": persoana,
        "garantie": garantie,
        "descriere": descriere,
        "id_anunt": id_final or "",
        "user_id": user_id or "",
        "localitate": locality or "",
        "vizualizari": viz,
        "vanzator": vanzator,
    }


# --- telefon ---
SHOW_PHONE_SELECTORS: List[Tuple[str, str]] = [
    (By.CSS_SELECTOR, "[data-testid='show-phone-number']"),
    (By.CSS_SELECTOR, "[data-cy='ad-contact-phone']"),
    (
        By.XPATH,
        "//button[contains(., 'AratÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢') or contains(., 'Arata') or contains(., 'Show')]",
    ),
]


def _phones_from_dom(driver) -> List[str]:
    phones = set()
    try:
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href^='tel:']"):
            href = a.get_attribute("href") or ""
            if href.lower().startswith("tel:"):
                ph = clean_phone(href.split(":", 1)[1])
                if ph:
                    phones.add(ph)
    except Exception:
        pass
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


def reveal_phone_robust(driver) -> List[str]:
    nums = _phones_from_dom(driver)
    if nums:
        return sorted(set(nums))
    for _ in range(3):
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
                        time.sleep(1.5)
                        nums = _phones_from_dom(driver)
                        if nums:
                            return sorted(set(nums))
            except Exception:
                continue
        if clicked:
            nums = _phones_from_dom(driver)
            if nums:
                return sorted(set(nums))
        accept_cookies_if_any(driver)
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


# ========= RUNNERS =========


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

        log_stage(
            "AD",
            "END OK",
            f"phones={len(phones)} | ad_id={fields.get('id_anunt')} | user_id={fields.get('user_id')} | loc={fields.get('localitate')}",
        )
        return fields, phones

    except Exception as e:
        log_stage("AD", "END FAIL", str(e))
        stat_inc("errors", 1)
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


class IncrementalWriters:
    def __init__(self, prefix: str, enable_jsonl: bool = True):
        ts = time.strftime("%Y%m%d-%H%M%S")
        self.csv_path = f"{prefix}_{ts}.csv"
        self.jsonl_path = f"{prefix}_{ts}.jsonl" if enable_jsonl else None
        self._csv_init = False
        self._csv_fh = None
        self._csv_writer = None
        self.cols = [
            "telefon",
            "titlu",
            "pret",
            "pret_valoare",
            "pret_moneda",
            "persoana",
            "garantie",
            "descriere",
            "id_anunt",
            "user_id",
            "localitate",
            "vizualizari",
            "vanzator",
            "url",
        ]
        self.rows_cache: List[Dict[str, str]] = (
            []
        )  # pentru Excel la final (opÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºional)

    def append(self, row: Dict[str, str]):
        if not self._csv_init:
            self._csv_fh = open(self.csv_path, "a", newline="", encoding="utf-8-sig")
            self._csv_writer = csv.DictWriter(self._csv_fh, fieldnames=self.cols)
            self._csv_writer.writeheader()
            self._csv_init = True
        self._csv_writer.writerow(row)
        self._csv_fh.flush()
        if self.jsonl_path:
            with open(self.jsonl_path, "a", encoding="utf-8") as jf:
                jf.write(json.dumps(row, ensure_ascii=False) + "\n")
                jf.flush()
        self.rows_cache.append(row)

    def close(self):
        try:
            if self._csv_fh:
                self._csv_fh.close()
        except Exception:
            pass

    def export_excel(self, xlsx_path: str):
        try:
            pd.DataFrame(self.rows_cache, columns=self.cols).to_excel(xlsx_path, index=False)
        except Exception as e:
            log.warning(f"Nu am putut scrie Excel: {e}")


# ========= RESUME: citeÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢te toate CSV-urile istorice ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i strÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢nge URL-urile deja procesate =========
def load_seen_urls_from_history(prefix: str) -> set[str]:
    seen = set()
    try:
        import glob

        for path in glob.glob(f"{prefix}_*.csv"):
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                # cautÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ o coloanÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ 'url' (sau 'link'/'href'); dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ nu existÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢, ia ultima coloanÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
                low = [fn.strip().lower() for fn in reader.fieldnames]
                url_key = None
                for key in ("url", "link", "href"):
                    if key in low:
                        url_key = reader.fieldnames[low.index(key)]
                        break
                for row in reader:
                    if url_key and row.get(url_key):
                        seen.add(row[url_key].strip())
                    elif row:  # fallback: ultima coloanÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
                        vals = list(row.values())
                        if vals:
                            seen.add(str(vals[-1]).strip())
    except Exception:
        pass
    return seen


# ========= MAIN =========


def main():
    init_run_logging()
    log_stage("RUN", "START", f"id={RUN_ID}")
    seeds = read_urls("urls.txt")
    pools = load_proxies("proxies.json")
    email, password = load_secrets("secrets.env")

    # ad_driver: sticky (autentificat)
    ad_ep = pools.ad_endpoints[0] if pools.ad_endpoints else None
    ad_driver = make_driver(ad_ep, pools.verify_ssl, ua=FIXED_AD_UA)
    ad_driver = ensure_single_login(ad_driver, email, password)
    ad_driver = health_check_driver(ad_driver, name="AD", email=email, password=password)

    # list_driver: poate folosi alt proxy (rotating)
    list_ep = pools.list_endpoints[0] if pools.list_endpoints else None
    list_driver = make_driver(list_ep, pools.verify_ssl)
    list_driver = health_check_driver(list_driver, name="LIST")

    writers = IncrementalWriters(OUTPUT_PREFIX, enable_jsonl=EXPORT_JSONL)
    # resume: URL-uri colectate ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®n rulÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ri anterioare (din CSV-urile istorice)
    seen_urls = load_seen_urls_from_history(OUTPUT_PREFIX)
    seen = set()
    visited = 0

    try:
        for seed in seeds:
            page_idx = 1
            while True:
                if MAX_PAGES_PER_SEED and page_idx > MAX_PAGES_PER_SEED:
                    break
                url = seed if page_idx == 1 else f"{seed.rstrip('/')}/?page={page_idx}"
                links: List[Tuple[str, str]] = []
                for attempt in range(1, MAX_PAGE_RETRIES + 1):
                    log_stage("LIST_PAGE", "EXECUTING", f"attempt={attempt}/{MAX_PAGE_RETRIES} | url={url}")
                    links = try_list_page(list_driver, url)
                    if links:
                        break
                    exp_backoff(attempt)
                if not links:
                    break

                total = len(links)

                stat_inc("links_total", total)
                for idx, (_, href) in enumerate(links, start=1):
                    # resume: dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ avem deja acest URL din istorice, sari
                    if href in seen_urls:
                        log_stage("AD", "SKIP", f"deja colectat | url={href}")

                        stat_inc("seen_skip", 1)
                        continue
                    log_stage("AD", "EXECUTING", f"[{idx}/{total}] url={href}")
                    fields: Dict[str, str]
                    phones: List[str]
                    stat_inc("ads_attempted", 1)
                    for attempt in range(1, MAX_AD_RETRIES + 1):
                        fields, phones = try_ad_page(ad_driver, href)
                        if fields.get("titlu") or phones:
                            break
                        try:
                            ad_driver.quit()
                        except Exception:
                            pass
                        ad_driver = make_driver(ad_ep, pools.verify_ssl, ua=FIXED_AD_UA)
                        load_cookies(ad_driver)
                        if not is_logged_in(ad_driver):
                            ensure_single_login(ad_driver, email, password)
                        exp_backoff(attempt)
                    visited += 1

                    if phones:
                        stat_inc("phones_found", 1)
                        stat_inc("ads_saved", 1)
                        seen_urls.add(href)

                    else:
                        key = ("", href)
                        if key in seen:
                            continue
                        seen.add(key)
                        writers.append({"telefon": "", **fields, "url": href})
                        stat_inc("ads_saved", 1)
                        seen_urls.add(href)

                    time.sleep(random.uniform(*JITTER))
                page_idx += 1

        # === EXPORT FINAL ===
        log_stage("EXPORT", "STARTING")
        ts = time.strftime("%Y%m%d-%H%M%S")
        xlsx_path = f"{OUTPUT_PREFIX}_{ts}.xlsx"
        writers.export_excel(xlsx_path)
        found = sum(1 for r in writers.rows_cache if r.get("telefon"))
        log_stage("EXPORT", "END OK", f"found={found} | ads_visited={visited} | CSV={writers.csv_path}")
        if writers.jsonl_path:
            log.info(f"JSONL: {writers.jsonl_path}")
        log.info(f"XLSX : {xlsx_path}")

    finally:
        try:
            list_driver.quit()
        except Exception:
            pass
        try:
            ad_driver.quit()
        except Exception:
            pass
        writers.close()


def health_check_driver(
    drv, name: str, email: str | None = None, password: str | None = None, homepage: str = "https://www.olx.ro/"
) -> webdriver.Chrome:
    """VerificÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ starea driverului; dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ e invalid, ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®l recreeazÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ ÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢i ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®l aduce ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â®ntr-o stare bunÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢."""
    log_stage("HEALTH", "STARTING", f"{name}")

    def _rebuild(current):
        log_stage("HEALTH", "REPAIR", f"{name} | recreez driver")
        try:
            current.quit()
        except Exception:
            pass
        # AD -> UA fix; LIST -> UA random (None)
        ua = FIXED_AD_UA if name.upper() == "AD" else None
        new_drv = make_driver(ep=None, verify_ssl=True, ua=ua)
        try:
            load_cookies(new_drv)
        except Exception:
            pass
        return new_drv

    # 1) sesiune validÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢? + cel puÃƒÆ’Ã†â€™Ãƒâ€¹Ã¢â‚¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Âºin un tab
    try:
        handles = (
            drv.window_handles
        )  # aruncÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ dacÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ sesiunea e moartÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢
    except (InvalidSessionIdException, WebDriverException, AttributeError):
        drv = _rebuild(drv)
        handles = drv.window_handles
    try:
        if not handles:
            drv.execute_script("window.open('about:blank','_blank');")
            drv.switch_to.window(drv.window_handles[-1])
    except Exception:
        pass

    # 2) homepage + cookies
    try:
        cur = ""
        try:
            cur = drv.current_url
        except Exception:
            cur = ""
        if not cur or cur in ("about:blank", "data:,"):
            drv.get(homepage)
        WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        accept_cookies_if_any(drv)
    except (InvalidSessionIdException, WebDriverException) as e:
        log_stage("HEALTH", "REPAIR", f"{name} | reload homepage: {e}")
        drv = _rebuild(drv)
        drv.get(homepage)
        WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        accept_cookies_if_any(drv)

    # 3) login pentru AD
    try:
        if name.upper() == "AD" and not is_logged_in(drv):
            drv = ensure_single_login(drv, email or "", password or "")
    except (InvalidSessionIdException, WebDriverException):
        drv = ensure_single_login(_rebuild(drv), email or "", password or "")

    log_stage("HEALTH", "END OK", f"{name} | url={drv.current_url}")
    return drv


if __name__ == "__main__":
    try:
        main()
    finally:
        log_stage(
            "RUN",
            "END",
            f"links={STATS['links_total']} saved={STATS['ads_saved']} skip_seen={STATS['seen_skip']} errors={STATS['errors']} phones={STATS['phones_found']} id={RUN_ID}",
        )
        finalize_run_index(extra=STATS)
