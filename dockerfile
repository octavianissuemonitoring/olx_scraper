# =========
# OLX SCRAPER - DOCKERFILE
# - Python 3.12 slim
# - Google Chrome Stable (headless) + Selenium Manager
# - Locale ro_RO.UTF-8, TZ Europe/Bucharest
# =========
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Europe/Bucharest \
    LANG=ro_RO.UTF-8 \
    LC_ALL=ro_RO.UTF-8

# OS deps + locales + Chrome
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl gnupg ca-certificates locales tzdata dumb-init \
      fonts-liberation fonts-noto fonts-noto-color-emoji \
    && sed -i 's/# ro_RO.UTF-8 UTF-8/ro_RO.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen ro_RO.UTF-8 \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    # Google Chrome repo
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
         | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
         > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends \
         google-chrome-stable \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*

# Directory aplicație
WORKDIR /app

# Instalăm întâi dependențele (layer cache-friendly)
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# Copiem restul surselor
COPY . /app

# Director output (dacă nu există)
RUN mkdir -p /app/out /app/out/debug

# (opțional) rulează ca user ne-root (evită --no-sandbox în Chrome)
# Dacă scriptul tău adaugă deja --no-sandbox, poți comenta acești 3 pași.
RUN useradd -m appuser
USER appuser

# Entry
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["python", "scraper_olx.py"]
