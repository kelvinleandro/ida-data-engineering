"""
Módulo de Configuração para o ETL.
"""
from pathlib import Path

# TODO: mover para arquivo .env
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "anatel_ida_db",
    "user": "user",
    "password": "password"
}

SCHEMA_NAME = "anatel_datamart"

DATA_DIRECTORY = Path("./data")

SERVICE_CONFIG = {
    "SMP": {"service_name": "Telefonia Celular", "service_acronym": "SMP", "skiprows": 8},
    "SCM": {"service_name": "Banda Larga Fixa", "service_acronym": "SCM", "skiprows": 8},
    "STFC": {"service_name": "Telefonia Fixa Local", "service_acronym": "STFC", "skiprows": 8},
}

# Regex para extrair informações do nome do arquivo
FILENAME_REGEX_PATTERN = r"^(SMP|SCM|STFC)(\d{4})\.ods$"