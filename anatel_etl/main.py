"""
Script Principal do ETL da Anatel.
Busca arquivos .ods no diretório de dados, extrai informações e os processa.
"""

import logging
import re
from anatel_etl.config import (
    DB_CONFIG,
    SCHEMA_NAME,
    DATA_DIRECTORY,
    SERVICE_CONFIG,
    FILENAME_REGEX_PATTERN,
)
from anatel_etl.db_manager import DBManager
from anatel_etl.etl_processor import ETLProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def find_and_process_files(db_manager: DBManager):
    """
    Encontra arquivos .ods no DATA_DIRECTORY, parseia o nome para obter metadados
    e inicia o processamento ETL para cada arquivo válido.

    Args:
        db_manager (DBManager): Instância do DBManager.
    """
    if not DATA_DIRECTORY.is_dir():
        logging.error(f"Diretório de dados especificado não existe: {DATA_DIRECTORY}")
        return

    file_pattern_compiled = re.compile(FILENAME_REGEX_PATTERN, re.IGNORECASE)

    found_files = 0
    processed_files = 0

    for file_path in DATA_DIRECTORY.glob("*.ods"):
        logging.info(f"Encontrado arquivo: {file_path.name}")
        found_files += 1

        match = file_pattern_compiled.match(file_path.name)
        if not match:
            logging.warning(
                f"Arquivo {file_path.name} não possui o padrão esperado. Ignorando arquivo..."
            )
            continue

        service_prefix = match.group(1).upper()
        year_str = match.group(2)

        try:
            year = int(year_str)
        except ValueError:
            logging.warning(
                f"Ano inválido '{year_str}' no nome do arquivo {file_path.name}. Ignorando arquivo..."
            )
            continue

        if service_prefix not in SERVICE_CONFIG:
            logging.warning(
                f"Prefixo de serviço '{service_prefix}' não reconhecido no arquivo {file_path.name}. "
                "Ignorando arquivo..."
            )
            continue

        current_service_config = SERVICE_CONFIG[service_prefix]

        processor = ETLProcessor(
            file_path=file_path,
            service_info=current_service_config,
            year=year,
            db_manager_instance=db_manager,
        )
        processor.process_file()
        processed_files += 1

    if found_files == 0:
        logging.info(f"Nenhum arquivo .ods encontrado em {DATA_DIRECTORY}")
    else:
        logging.info(
            f"Total de arquivos .ods encontrados: {found_files}. Total processados com sucesso (iniciado): {processed_files}."
        )


def run_etl_pipeline():
    """
    Executa o pipeline ETL completo.
    """
    logging.info(">>> Iniciando Pipeline ETL <<<")
    db_manager = None
    try:
        db_manager = DBManager(DB_CONFIG, SCHEMA_NAME)
        db_manager.connect()
        find_and_process_files(db_manager)
    except Exception as e:
        logging.critical(f"Ocorreu um erro no pipeline ETL: {e}", exc_info=True)
    finally:
        if db_manager and db_manager.conn:
            db_manager.close()
        logging.info(">>> Pipeline ETL finalizado <<<")


if __name__ == "__main__":
    run_etl_pipeline()
