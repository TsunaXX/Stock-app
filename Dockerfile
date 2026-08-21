FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Goodinfo fallback and PDF/report tools need a real Chromium runtime on the
# server.  The application still works when Goodinfo is unavailable; these
# packages simply preserve the existing browser-based fallback.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        chromium chromium-driver fonts-noto-cjk ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY app.py stock_names.csv google_apps_script.gs ./
COPY .streamlit/config.toml ./.streamlit/config.toml

EXPOSE 10000

CMD ["sh", "-c", "exec streamlit run app.py --server.address=0.0.0.0 --server.port=${PORT:-10000}"]
