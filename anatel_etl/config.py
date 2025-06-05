"""
Módulo de Configuração para o ETL.
"""

import os
from pathlib import Path

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "anatel_ida_db"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

SCHEMA_NAME = "anatel_datamart"

DATA_DIRECTORY = Path(os.getenv("DATA_DIR_CONTAINER", "/app/data"))

SERVICE_CONFIG = {
    "SMP": {
        "service_name": "Telefonia Celular",
        "service_acronym": "SMP",
        "skiprows": 8,
    },
    "SCM": {
        "service_name": "Banda Larga Fixa",
        "service_acronym": "SCM",
        "skiprows": 8,
    },
    "STFC": {
        "service_name": "Telefonia Fixa Local",
        "service_acronym": "STFC",
        "skiprows": 8,
    },
}

# Regex para extrair informações do nome do arquivo
FILENAME_REGEX_PATTERN = r"^(SMP|SCM|STFC)(\d{4})\.ods$"
