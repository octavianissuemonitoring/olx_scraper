# test_proxy.py — Test simplu pentru GeoNode + Selenium Wire + Selenium Manager (fără webdriver_manager)
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options

# === COMPLETEAZĂ CU DATELE TALE ===
PROTOCOL = "http"     # "http" pentru HTTP/HTTPS sau "socks5" pentru SOCKS5
HOST     = "proxy.geonode.io"
PORT     = 9000       # număr (ex. 9000 pentru http/https, 1080 pt. socks5)
USERNAME = "geonode_3UlswT3blD"     # ex. "geonode_3UlswT3blD-type-residential"
PASSWORD = "41a8aed7-f884-4940-b016-af7c82c684a8"     # ex. "41a8aed7-f884-4940-b016-af7c82c684a8"

# Dacă folosești whitelist IP și NU ai user/parolă:
# USERNAME, PASSWORD = "", ""

# Construim URL-urile de proxy pentru selenium-wire
if PROTOCOL.lower() == "http":
    PROXY_HTTP  = f"http://{USERNAME+':' if USERNAME else ''}{PASSWORD+'@' if USERNAME else ''}{HOST}:{PORT}"
    PROXY_HTTPS = f"https://{USERNAME+':' if USERNAME else ''}{PASSWORD+'@' if USERNAME else ''}{HOST}:{PORT}"
elif PROTOCOL.lower() == "socks5":
    PROXY_HTTP  = f"socks5://{USERNAME+':' if USERNAME else ''}{PASSWORD+'@' if USERNAME else ''}{HOST}:{PORT}"
    PROXY_HTTPS = PROXY_HTTP
else:
    raise ValueError("Protocol necunoscut. Folosește 'http' sau 'socks5'.")

seleniumwire_options = {
    "proxy": {
        "http": PROXY_HTTP,
        "https": PROXY_HTTPS,
    },
    # Dacă primești erori SSL de la proxy, setează temporar False:
    # "verify_ssl": False,
}

opts = Options()
opts.add_argument("--window-size=1200,800")
# opțional: UA prietenos
opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36")

# Folosim Selenium Manager (integrat în Selenium 4.6+) — nu avem nevoie de webdriver_manager
driver = webdriver.Chrome(seleniumwire_options=seleniumwire_options, options=opts)

try:
    # 1) api.ipify.org -> ar trebui să printăm IP-ul public (cel al PROXY-ului)
    driver.get("https://api.ipify.org?format=json")
    print("ipify:", driver.page_source)

    # 2) httpbin.org/ip — alternativă pentru verificare
    driver.get("https://httpbin.org/ip")
    print("httpbin:", driver.page_source)
finally:
    driver.quit()
