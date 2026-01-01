from db.connection import Database
from psycopg2 import sql

class ManagerDb(Database):

    def __init__(self):
        super().__init__()
        self.conn = self._conn()

    @property
    def conn_to_database(self):
        return self.conn
    
    def _delete_table(self, table_name: str, schema_name: str) -> str:
        """
        Deleta uma tabela do banco de dados.
        
        Args:
            table_name (str): Nome da tabela a ser deletada.
            schema_name (str): Nome do schema onde a tabela está localizada.

        Exception: 
            Trata erros de conexão ou outras variáveis.
        Finally: 
            Fecha conexão com o banco de dados.
        """
        try:
            query = sql.SQL(
                """
                DROP TABLE IF EXISTS {}.{} CASCADE;
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            )

            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao deletar a tabela {table_name}: {e}")
        finally:
            self.connection.close()

    def _delete_schema(self, schema_name: str) -> str:
        """
        Deleta um schema do banco de dados.

        Args:
            schema_name (str): Nome do schema a ser deletado.

        Exception: 
            Trata erros de conexão ou outras variáveis.
        Finally: 
            Fecha conexão com o banco de dados.
        """
        try:
            query = sql.SQL(
                """
                DROP SCHEMA IF EXISTS {} CASCADE;
                """
            ).format(
                sql.Identifier(schema_name)
            )

            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao deletar o schema {schema_name}: {e}")
        finally:
            self.connection.close()


    def _delete_column(self, table_name: str, schema_name: str, column_name: str) -> str:
        """
        Deleta uma coluna de uma tabela no banco de dados.

        Args:
            table_name (str): Nome da tabela onde a coluna será deletada.
            schema_name (str): Nome do schema onde a tabela está localizada.
            column_name (str): Nome da coluna a ser deletada.

        Exception: 
            Trata erros de conexão ou outras variáveis.
        Finally: 
            Fecha conexão com o banco de dados.
        """
        try:
            query = sql.SQL(
                """
                ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.Identifier(column_name)
            )

            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao deletar a coluna {column_name} da tabela {table_name}: {e}")
        finally:
            self.connection.close()