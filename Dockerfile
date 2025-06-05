FROM python:3.11.12-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR_CONTAINER=/app/data

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./anatel_etl ./anatel_etl

CMD ["python", "-m", "anatel_etl.main"]