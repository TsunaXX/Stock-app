FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# PDF/report rendering needs CJK fonts and current certificate authorities.
# Goodinfo uses a bounded HTTP request so the free 512 MB instance does not
# launch a memory-heavy Chromium process during an interactive rerun.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fonts-noto-cjk ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY app.py stock_names.csv google_apps_script.gs ./
COPY .streamlit/config.toml ./.streamlit/config.toml

EXPOSE 10000

CMD ["sh", "-c", "exec streamlit run app.py --server.address=0.0.0.0 --server.port=${PORT:-10000}"]
