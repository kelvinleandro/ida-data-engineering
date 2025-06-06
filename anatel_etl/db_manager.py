"""
Módulo Gerenciador do Banco de Dados.

Responsável por todas as interações com o banco de dados PostgreSQL,
incluindo conexões, inserções e consultas para popular dimensões e fatos.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DBManager:
    """
    Gerencia a conexão e as operações com o banco de dados PostgreSQL.
    """

    def __init__(self, db_config: dict, schema_name: str):
        """
        Inicializa o DBManager com as configurações do banco de dados.

        Args:
            db_config (dict): Dicionário com 'host', 'port', 'database', 'user', 'password'.
            schema_name (str): Nome do schema do Data Mart.
        """
        self.db_config = db_config
        self.schema_name = schema_name
        self.conn = None
        self.cursor = None

    def connect(self):
        """Estabelece a conexão com o banco de dados."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logging.info("Conexão com o PostgreSQL estabelecida com sucesso.")
        except psycopg2.Error as e:
            logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise

    def close(self):
        """Fecha a conexão com o banco de dados."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logging.info("Conexão com o PostgreSQL fechada.")

    def _get_or_create_id(
        self, table_name: str, columns_data: dict, query_columns: list
    ) -> int:
        """
        Busca um ID existente ou cria uma nova entrada na tabela de dimensão.

        Args:
            table_name (str): Nome da tabela de dimensão (sem o schema).
            columns_data (dict): Dicionário com {nome_coluna: valor} para inserção/consulta.
            query_columns (list): Lista de colunas para usar na cláusula WHERE para verificar existência.

        Returns:
            int: O ID da entrada na tabela de dimensão.
        """
        id_column_name = f"id_{table_name.replace('dim_', '').lower()}"

        full_table_name = sql.SQL("{}.{}").format(
            sql.Identifier(self.schema_name), sql.Identifier(table_name)
        )

        select_conditions = sql.SQL(" AND ").join(
            [sql.SQL("{} = %s").format(sql.Identifier(col)) for col in query_columns]
        )
        select_values = [columns_data[col] for col in query_columns]

        select_query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.Identifier(id_column_name), full_table_name, select_conditions
        )

        try:
            self.cursor.execute(select_query, select_values)
            result = self.cursor.fetchone()

            if result:
                return result[0]
            else:
                insert_cols_ident = [sql.Identifier(col) for col in columns_data.keys()]
                insert_vals_placeholders = sql.SQL(", ").join(
                    sql.Placeholder() * len(columns_data)
                )

                insert_query = sql.SQL(
                    "INSERT INTO {} ({}) VALUES ({}) RETURNING {}"
                ).format(
                    full_table_name,
                    sql.SQL(", ").join(insert_cols_ident),
                    insert_vals_placeholders,
                    sql.Identifier(id_column_name),
                )

                self.cursor.execute(insert_query, list(columns_data.values()))
                self.conn.commit()
                return self.cursor.fetchone()[0]
        except psycopg2.Error as e:
            self.conn.rollback()
            logging.error(
                f"Erro em _get_or_create_id para {table_name} com dados {columns_data}. Erro: {e}"
            )
            logging.error(
                f"Query: {self.cursor.query if self.cursor else 'Cursor não disponível'}"
            )
            raise

    def get_or_create_tempo_id(self, ano_mes: str) -> int:
        """
        Obtém ou cria um ID para dim_tempo.
        Ignora trimestre e nome_mes conforme solicitado.

        Args:
            ano_mes (str): String no formato 'YYYY-MM'.

        Returns:
            int: O id_tempo.
        """
        try:
            year, month = map(int, ano_mes.split("-"))
            data_completa = datetime(year, month, 1).date()

            return self._get_or_create_id(
                table_name="dim_tempo",
                columns_data={
                    "ano_mes": ano_mes,
                    "ano": year,
                    "mes": month,
                    "data_completa": data_completa,
                },
                query_columns=["ano_mes"],
            )
        except ValueError as e:
            logging.error(f"Formato de ano_mes inválido: {ano_mes}. Erro: {e}")
            raise

    def get_or_create_grupo_economico_id(self, nome_grupo: str):
        """Obtém ou cria um ID para dim_grupo_economico."""
        return self._get_or_create_id(
            table_name="dim_grupo_economico",
            columns_data={"nome_grupo_economico": nome_grupo},
            query_columns=["nome_grupo_economico"],
        )

    def get_or_create_indicador_id(self, nome_indicador: str):
        """Obtém ou cria um ID para dim_indicador."""
        return self._get_or_create_id(
            table_name="dim_indicador",
            columns_data={"nome_indicador": nome_indicador},
            query_columns=["nome_indicador"],
        )

    def get_or_create_servico_id(self, nome_servico: str, sigla_servico: str):
        """Obtém ou cria um ID para dim_servico."""
        return self._get_or_create_id(
            table_name="dim_servico",
            columns_data={"nome_servico": nome_servico, "sigla_servico": sigla_servico},
            query_columns=["sigla_servico"],
        )

    def bulk_insert_facts(self, fact_data_list):
        """
        Insere múltiplos registros na tabela fact_indicador_desempenho (sem id_localidade).

        Args:
            fact_data_list (list of tuples): (id_tempo, id_grupo_economico, id_servico,
                                              id_indicador, valor).
        """
        if not fact_data_list:
            logging.info("Nenhum dado de fato para inserir.")
            return

        query_str = (
            f"INSERT INTO {self.schema_name}.fact_indicador_desempenho "
            "(id_tempo, id_grupo_economico, id_servico, id_indicador, valor) "
            "VALUES %s"
        )

        try:
            execute_values(self.cursor, query_str, fact_data_list)
            self.conn.commit()
            logging.info(
                f"{len(fact_data_list)} registros inseridos na tabela de fatos."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logging.error(f"Erro ao inserir dados na tabela de fatos: {e}")
            logging.error(f"Tentativa de query: {query_str[:200]}...")
            raise
