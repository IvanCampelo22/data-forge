from psycopg2 import sql
from db.connection import Database
from typing import Any, Dict
from db.clipping_db.get_table_news import GetNews
from loguru import logger

class EditRegisters(Database):

    def __init__(self):
        super().__init__()
        self.conn = self._conn()

    @property
    def conn_to_database(self):
        return self.conn

    def _mark_as_deleted_by_id(self, table_name: str, schema_name: str, record_id: int):
        """
        Marca um registro como is_deleted = TRUE e desativa a notícia (is_active = FALSE).
        
        Args:
            table_name (str): Nome da tabela a ser consultada.
            schema_name (str): Nome do schema no banco.
            record_id (int): Identificador do registro.

        Exception:
            Faz um rollback para garantir que nenhum registro com problema seja salvo.
            Retorna um erro sobre o problema da consulta.
        """
        try:
            query = sql.SQL("""
                UPDATE {}.{}
                SET is_deleted = TRUE
                WHERE id = %s
                RETURNING news_code;
            """).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            )
            
            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query, (record_id,))
                result = cursor.fetchone()
                self.connection.commit()

                if result:
                    news_code = result[0]
                    logger.error(news_code)  # Obtém o news_code do registro
                    GetNews()._deactivate_clipping_news(news_code)  # 🔹 Chama a função para desativar a notícia

            return result

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao marcar registro {record_id} como deletado: {e}")

        finally:
            if self.connection:
                self.connection.close()
        
    def _get_deleted_records(self, table_name: str, schema_name: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Filtra os registros que estão com o is_deleted marcado como True, com suporte a paginação.

        Args:
            table_name (str): Nome da tabela a ser consultada.
            schema_name (str): Nome do schema da tabela.
            limit (int): Número máximo de registros retornados por página.
            offset (int): Número de registros a serem ignorados antes de retornar os resultados.

        Returns:
            Um dicionário contendo os registros deletados paginados e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro na consulta ao banco de dados.
        """
        try:
            # Query para buscar registros deletados com paginação
            query = f"""
                SELECT * FROM {schema_name}.{table_name}
                WHERE is_deleted = TRUE
                ORDER BY deleted_at DESC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de registros deletados
            count_query = f"""
                SELECT COUNT(*)
                FROM {schema_name}.{table_name}
                WHERE is_deleted = TRUE;
            """

            with self.conn_to_database.cursor() as cursor:
                # Executa a query de contagem total
                cursor.execute(count_query)
                total_records = cursor.fetchone()[0]

                # Executa a query paginada
                cursor.execute(query, (limit, offset))
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            # Converte os resultados para um formato de dicionário
            result = [dict(zip(columns, row)) for row in rows]

            return {
                "total_records": total_records,
                "page_size": limit,
                "current_offset": offset,
                "trash": result
            }

        except Exception as e:
            self.conn_to_database.rollback()
            raise Exception(f"Erro ao consultar registros marcados como is_deleted: {e}")

        finally:
            if self.conn_to_database:
                self.conn_to_database.close()

    def _get_active_record(self, table_name: str, schema_name: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Filtra os registros que estão com is_deleted marcado como False, com suporte a paginação.

        Args:
            table_name (str): Nome da tabela a ser consultada.
            schema_name (str): Nome do schema da tabela.
            limit (int): Número máximo de registros retornados por página.
            offset (int): Número de registros a serem ignorados antes de retornar os resultados.

        Returns:
            Um dicionário contendo os registros ativos paginados e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro na consulta ao banco de dados.
        """
        try:
            # Query para buscar registros ativos com paginação
            query = f"""
                SELECT * FROM {schema_name}.{table_name}
                WHERE is_deleted = FALSE
                ORDER BY id ASC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de registros ativos
            count_query = f"""
                SELECT COUNT(*)
                FROM {schema_name}.{table_name}
                WHERE is_deleted = FALSE;
            """

            with self.conn.cursor() as cursor:
                # Executa a query de contagem total
                cursor.execute(count_query)
                total_records = cursor.fetchone()[0]

                # Executa a query paginada
                cursor.execute(query, (limit, offset))
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            # Converte os resultados para um formato de dicionário
            result = [dict(zip(columns, row)) for row in rows]

            return {
                "total_records": total_records,
                "page_size": limit,
                "current_offset": offset,
                "active_records": result
            }

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Erro ao consultar registros ativos: {e}")

        finally:
            if self.conn:
                self.conn.close()
    
    def _rename_field_with_validation(self, table_name: str, schema_name: str, old_field_name: str, new_field_name: str) -> str:
        """
        Renomeia uma coluna em uma tabela, validando os dados antes e depois da alteração.
        Args:
            table_name (str): Nome da tabela.
            old_field_name (str): Nome atual da coluna.
            new_field_name (str): Novo nome desejado para a coluna.    

        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        
        Finally:
            Encerra conexão com o banco de dados.
            
        """
        if not table_name or not old_field_name or not new_field_name:
            raise ValueError("Os parâmetros 'table_name', 'old_field_name' e 'new_field_name' não podem ser nulos ou vazios.")

        try:
            validate_query = f"""
                SELECT COUNT(*) AS total_records, COUNT({old_field_name})
                FROM {schema_name}.{table_name};
            """
            with self.conn.cursor() as cursor:
                cursor.execute(validate_query)
                result = cursor.fetchone()
                total_records, non_null_records = result

            rename_query = f"""
                ALTER TABLE {schema_name}.{table_name}
                RENAME COLUMN {old_field_name} TO {new_field_name};
            """
            with self.conn.cursor() as cursor:
                cursor.execute(rename_query)
                self.connection.commit()

            return f"Coluna renomeada de '{old_field_name}' para '{new_field_name}' com sucesso."

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao renomear coluna: {e}")
        finally:
            if self.connection:
                self.connection.close()

    def _change_field_type_with_validation(self, table_name: str, schema_name: str, field_name: str, new_field_type: str):
        """
        Atualiza o tipo de um campo, com algumas validações para assegurar que está tudo certo.
        Args:
            table_name (str): Nome da tabela onde o novo campo vai ser inserido
            field_name (str): Nome do campo a ser atualizado
            field_type (str): Novo tipo do campo  

        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        
        Finally:
            Encerra conexão com o banco de dados.
            
        """
        if not table_name or not field_name or not new_field_type:
            raise ValueError("Os parâmetros 'table_name', 'field_name' e 'new_field_type' não podem ser nulos ou vazios.")

        try:
            alter_query = f"""
                ALTER TABLE {schema_name}.{table_name}
                ALTER COLUMN {field_name} TYPE {new_field_type}
                USING {field_name}::{new_field_type};
            """

            with self.conn.cursor() as cursor:
                cursor.execute(alter_query)
                self.connection.commit()

            return f"Tipo da coluna '{field_name}' alterado para '{new_field_type}' com sucesso."

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao alterar o tipo da coluna: {e}")

        finally:
            if self.connection:
                self.connection.close()

    def _get_deleted_records_by_date_range(self, table_name: str, schema_name: str, start_date: str, end_date: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Filtra registros deletados (is_deleted = TRUE) pelo range de datas, com suporte a paginação.

        Args:
            table_name (str): Nome da tabela a ser consultada.
            schema_name (str): Nome do schema da tabela.
            start_date (str): Data inicial no formato 'YYYY-MM-DD'.
            end_date (str): Data final no formato 'YYYY-MM-DD'.
            limit (int): Número máximo de registros retornados por página.
            offset (int): Número de registros a serem ignorados antes de retornar os resultados.

        Returns:
            Um dicionário contendo os registros deletados paginados e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro na consulta ao banco de dados.
        """
        try:
            # Query para buscar registros deletados com paginação
            query = f"""
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE is_deleted = TRUE
                AND date BETWEEN %s AND %s
                ORDER BY date DESC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de registros deletados no período
            count_query = f"""
                SELECT COUNT(*)
                FROM {schema_name}.{table_name}
                WHERE is_deleted = TRUE
                AND date BETWEEN %s AND %s;
            """

            with self.conn.cursor() as cursor:
                # Executa a query de contagem total
                cursor.execute(count_query, (start_date, end_date))
                total_records = cursor.fetchone()[0]

                # Executa a query paginada
                cursor.execute(query, (start_date, end_date, limit, offset))
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            # Converte os resultados para um formato de dicionário
            result = [dict(zip(columns, row)) for row in rows]

            return {
                "total_records": total_records,
                "page_size": limit,
                "current_offset": offset,
                "deleted_records": result
            }

        except Exception as e:
            raise Exception(f"Erro ao consultar registros deletados: {str(e)}")

        finally:
            if self.conn:
                self.conn.close()

    def _active_register(self, table_name: str, schema_name: str, record_id: int):
        """
        Marca um registro como is_deleted = FALSE e ativa a notícia (is_active = TRUE).
        
        Args:
            table_name (str): Nome da tabela a ser consultada.
            schema_name (str): Nome do schema no banco.
            record_id (int): Identificador do registro.

        Exception:
            Faz um rollback para garantir que nenhum registro com problema seja salvo.
            Retorna um erro sobre o problema da consulta.
        """
        try:
            query = sql.SQL("""
                UPDATE {}.{}
                SET is_deleted = FALSE
                WHERE id = %s
                RETURNING news_code;
            """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            )
            
            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query, (record_id,))
                result = cursor.fetchone()
                self.connection.commit()

                if result:
                    news_code = result[0]
                    logger.info(f"Ativando notícia com news_code: {news_code}")  # Obtém o news_code do registro
                    GetNews()._active_clipping_news(news_code)  # 🔹 Chama a função para ativar a notícia

            return result

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao ativar registro {record_id}: {e}")

        finally:
            if self.connection:
                self.connection.close()