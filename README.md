# OLX Scraper (autorulote)

Pipeline complet pentru listă → detaliu anunț, cu extragerea numerelor de telefon, export CSV/XLSX, proxy Geonode, sesiune OLX persistentă, retry/backoff și QA (pre-commit + pytest).

## Cerințe
- Windows 10/11, PowerShell
- Python 3.13 (recomandat) și virtualenv (`.venv`)
- Google Chrome
- Git (pentru versionare/CI)
- Cont OLX (opțional, pentru rate-limit mai permisiv după login)

## Setup rapid
```powershell
cd "C:\Users\octavian\OneDrive - My Organization\Desktop\olx_scraper"
py -m venv .venv
.\.venv\Scripts\activate
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m pip install -r requirements-dev.txt
py -m pre_commit install
