from db.connection import Database
from psycopg2 import sql

class DeveloperDb(Database):

    def __init__(self):
        super().__init__()
        self.conn = self._conn()

    @property
    def conn_to_database(self):
        return self.conn
    

    def _create_company_table(self) -> str:
        """
        Cria a tabela 'company' no banco de dados
        Exception:
            Trata erros de conexão ou outras variáveis
        Finally:
            Fecha conexão com o banco de dados
        """
        try:
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS company (
                    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name VARCHAR(255),
                    cnpj VARCHAR(20),
                    email_company VARCHAR(255),
                    is_active BOOLEAN
                );
                """
            )
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar tabela: {e}")
        finally:
            self.connection.close()
    
