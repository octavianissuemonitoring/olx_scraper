import re
from typing import Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Regex-uri folosite în proiect
PHONE_RE = re.compile(
    r"(?:\+?4?0\s*7[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})" r"|(?:07[\s\-.]?\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3})"
)

RE_ID = re.compile(r"\bID(?:-ul)?(?:\s*anuntului)?\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
RE_VIEWS = re.compile(r"\bVizualizari\s*[:#]?\s*([\d\s\.]+)", re.IGNORECASE)
# Permite: "Garantie (RON): 5 000", "Garantie: 5 000 RON", "garantie- 5000 Lei", etc.
# Logica: după cuv. "Garantie", acceptă până la 40 de caractere non-cifră (paranteze/colon/spații),
# apoi CAPTUREAZĂ un număr care începe cu cel puțin o cifră, urmată de cifre/spații/puncte,
# și opțional moneda după (RON/Lei/EUR/€).
RE_GARANTIE = re.compile(
    r"\bGarantie\b[^\d]{0,40}(\d[\d\s\.]*)(?:\s*(?:RON|Lei|EUR|€))?",
    re.IGNORECASE,
)


def parse_price(raw: str) -> Tuple[str, str]:
    """Întoarce (valoare, monedă) din texte precum '109 €' sau '5 000 Lei'."""
    if not raw:
        return "", ""
    m = re.search(r"([\d\.\s]+)\s*([€EeUuRrOo]|RON|Lei|LEI)?", raw)
    if not m:
        return "", ""
    val = re.sub(r"[^\d]", "", m.group(1) or "").strip()
    cur = (m.group(2) or "").upper().replace("LEI", "RON").replace("EURO", "€")
    if cur in ["E", "EURO"]:
        cur = "€"
    return val, cur or ""


def clean_phone(s: str) -> str:
    """Normalizează numărul la format RO 07xxxxxxxx."""
    digits = re.sub(r"\D", "", s or "")
    if digits.startswith("40") and len(digits) == 11 and digits[2] == "7":
        digits = "0" + digits[2:]
    if digits.startswith("7") and len(digits) == 9:
        digits = "0" + digits
    return digits


def normalize_url(href: str) -> str:
    """Normalizează URL-ul: scoate #frag și param. de tracking uzuali."""
    try:
        href = href.split("#", 1)[0]
        s = urlsplit(href)
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
