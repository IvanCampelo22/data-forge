from db.connection import Database
from psycopg2 import sql

class InsertColumn(Database):

    def __init__(self) -> None:
        super().__init__()
        self.conn = self._conn()

    @property
    def conn_to_database(self):
        return self.conn

    def _insert_column_text(self, table_name: str, schema_name: str, new_column: str):
        """
        Criar coluna de texto em uma tabela específica
        Args:
            table_name (str): Nome da tabela onde o novo campo vai ser inserido
            schema_name (str): Nome do schema onde a tabela está localizada.
            column_name (str): Nome da coluna a ser criada
        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        Finally: 
            Fecha conexão com o banco de dados
        """
        try: 
            query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} VARCHAR(1250);").format(sql.Identifier(schema_name), sql.Identifier(table_name), sql.Identifier(new_column))
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")

    
    def _insert_column_integer_number(self, table_name, schema_name, new_column):
        """
        Criar coluna de número inteiro em uma tabela específica
        Args:
            table_name (str): Nome da tabela onde o novo campo vai ser inserido
            column_name (str): Nome da coluna a ser criada
        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        Finally:
            Fecha conexão com o banco de dados
        """
        try:
            query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} INTEGER;").format(sql.Identifier(schema_name), sql.Identifier(table_name), sql.Identifier(new_column))
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()
    
    def _insert_column_float_number(self, table_name, schema_name, new_column):
        """
        Criar coluna de número float em uma tabela específica
        Args:
            table_name (str): Nome da tabela onde o novo campo vai ser inserido
            column_name (str): Nome da coluna a ser criada
        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        Finally:
            Fecha conexão com o banco de dados
        """
        try:
            query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} FLOAT;").format(sql.Identifier(schema_name), sql.Identifier(table_name), sql.Identifier(new_column))
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()
    
    def _insert_column_date(self, table_name, schema_name, new_column):
        """Criar coluna de número date em uma tabela específica
        Args:
            table_name (str): Nome da tabela onde o novo campo vai ser inserido
            column_name (str): Nome da coluna a ser criada
        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        Finally:
            Fecha conexão com o banco de dados
        """
        try:
            query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} DATE;").format(sql.Identifier(schema_name), sql.Identifier(table_name), sql.Identifier(new_column))
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()
    
    def _insert_column_boolean(self, table_name, schema_name, new_column):
        """Criar coluna de boolena date em uma tabela específica
        Args:
            table_name (str): Nome da tabela onde o novo campo vai ser inserido
            column_name (str): Nome da coluna a ser criada
        Exception: 
            Faz um rollback para garantir que nenhum registro com problema seja salvo
            Retorna um erro sobre o problema da consulta
        Finally:
            Fecha conexão com o banco de dados
        """
        try:
            query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} BOOLEAN;").format(sql.Identifier(schema_name), sql.Identifier(table_name), sql.Identifier(new_column))
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()

    