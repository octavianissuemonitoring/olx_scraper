import os
import sys

# tests/       → acest fișier (conftest.py)
# proiect/     → un nivel mai sus (root-ul unde ai scraper_olx.py)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
