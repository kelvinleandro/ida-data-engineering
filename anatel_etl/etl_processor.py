"""
Módulo Processador ETL.
"""

import logging
from pathlib import Path
import pandas as pd

from anatel_etl.db_manager import DBManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ETLProcessor:
    """
    Processa um arquivo de dados da Anatel (.ods), transformando e carregando-o
    no Data Mart.
    """

    def __init__(
        self,
        file_path: Path,
        service_info: dict[str, str],
        year: int,
        db_manager_instance: DBManager,
    ):
        """
        Inicializa o ETLProcessor.

        Args:
            file_path (pathlib.Path): Caminho para o arquivo .ods.
            service_info (dict): Configurações do serviço
                                 (service_name, service_acronym, skiprows).
            year (int): Ano extraído do nome do arquivo.
            db_manager_instance (DBManager): Instância do DBManager.
        """
        self.file_path = file_path
        self.skiprows = service_info["skiprows"]
        self.service_name = service_info["service_name"]
        self.service_acronym = service_info["service_acronym"]
        self.year = year
        self.db_manager = db_manager_instance
        self.months_columns = [f"{self.year}-{str(m).zfill(2)}" for m in range(1, 13)]

    def _extract_data(self):
        """
        Extrai dados do arquivo .ods usando pandas com engine odfpy.

        Returns:
            pandas.DataFrame or None: DataFrame com dados brutos ou None em caso de erro.
        """
        try:
            df = pd.read_excel(self.file_path, engine="odfpy", skiprows=self.skiprows)
            df.dropna(how="all", inplace=True)
            logging.info(f"Dados extraídos com sucesso de: {self.file_path}")
            return df
        except FileNotFoundError:
            logging.error(f"Arquivo não encontrado: {self.file_path}")
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo ODS {self.file_path}: {e}")
        return None

    def _transform_data(self, df_raw: pd.DataFrame):
        """
        Transforma os dados brutos: renomeia, derrete, limpa valores.

        Args:
            df_raw (pandas.DataFrame): DataFrame com os dados brutos.

        Returns:
            pandas.DataFrame: DataFrame transformado.
        """
        if df_raw is None or df_raw.empty:
            logging.warning(
                f"DataFrame bruto está vazio para {self.file_path}. Transformação pulada."
            )
            return pd.DataFrame()

        df_raw.rename(
            columns={
                "GRUPO ECONÔMICO": "nome_grupo_economico",
                "VARIÁVEL": "nome_indicador",
            },
            inplace=True,
        )

        id_vars = ["nome_grupo_economico", "nome_indicador"]

        actual_months_to_melt = [
            col for col in self.months_columns if col in df_raw.columns
        ]

        if not actual_months_to_melt:
            logging.warning(
                f"Nenhuma coluna de mês esperada ({self.months_columns}) "
                f"encontrada no arquivo {self.file_path}. "
                f"Colunas disponíveis: {df_raw.columns.tolist()}. "
            )
            return pd.DataFrame()

        logging.info(
            f"Colunas de meses a serem convertidas em formato longo no arquivo {self.file_path} (ano {self.year}): {actual_months_to_melt}"
        )

        df_long = pd.melt(
            df_raw,
            id_vars=id_vars,
            value_vars=actual_months_to_melt,
            var_name="ano_mes",
            value_name="valor",
        )

        df_long.dropna(subset=["valor"], inplace=True)

        logging.info(
            f"Dados transformados para {self.file_path}. {len(df_long)} linhas válidas."
        )
        return df_long

    def _load_data(self, df_transformed: pd.DataFrame):
        """
        Carrega os dados transformados no Data Mart.

        Args:
            df_transformed (pandas.DataFrame): DataFrame com dados transformados.
        """
        if df_transformed.empty:
            logging.info(
                f"Nenhum dado transformado para carregar para {self.file_path}."
            )
            return

        fact_data_list = []
        try:
            id_servico = self.db_manager.get_or_create_servico_id(
                self.service_name, self.service_acronym
            )
        except Exception as e:
            logging.error(
                f"Falha ao obter/criar id_servico para {self.service_name}. Abortando carga para {self.file_path}. Erro: {e}"
            )
            return

        for _, row in df_transformed.iterrows():
            try:
                if (
                    pd.isna(row.get("ano_mes"))
                    or pd.isna(row.get("nome_grupo_economico"))
                    or pd.isna(row.get("nome_indicador"))
                ):
                    logging.warning(
                        f"Linha com dados essenciais ausentes: {row}. Pulando."
                    )
                    continue

                id_tempo = self.db_manager.get_or_create_tempo_id(row["ano_mes"])
                id_grupo_economico = self.db_manager.get_or_create_grupo_economico_id(
                    row["nome_grupo_economico"]
                )
                id_indicador = self.db_manager.get_or_create_indicador_id(
                    row["nome_indicador"]
                )

                fact_data_list.append(
                    (
                        id_tempo,
                        id_grupo_economico,
                        id_servico,
                        id_indicador,
                        row["valor"],
                    )
                )
            except Exception as e:
                logging.error(
                    f"Erro ao processar linha para FKs: {row}. Erro: {e}. Pulando esta linha."
                )
                continue

        if fact_data_list:
            self.db_manager.bulk_insert_facts(fact_data_list)
        else:
            logging.info(f"Nenhuma linha de fato válida gerada para {self.file_path}.")

    def process_file(self):
        """
        Executa o pipeline ETL completo para o arquivo configurado.
        """
        logging.info(
            f"--- Iniciando processamento do arquivo: {self.file_path} (Serviço: {self.service_acronym}, Ano: {self.year}) ---"
        )
        df_raw = self._extract_data()
        if df_raw is not None and not df_raw.empty:
            df_transformed = self._transform_data(df_raw)
            if not df_transformed.empty:
                self._load_data(df_transformed)
        else:
            logging.warning(
                f"Extração resultou em DataFrame vazio ou erro para {self.file_path}. Processamento do arquivo interrompido."
            )
        logging.info(f"--- Processamento do arquivo {self.file_path} concluído ---")
