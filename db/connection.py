import psycopg2
from config.config import DB_HOST_HANDSON, DB_NAME_HANDSON, DB_PASSWORD_HANDSON, DB_USER_HANDSON


class Database:

    def __init__(self):
        self.db_name = DB_NAME_HANDSON
        self.db_user = DB_USER_HANDSON
        self.db_password = DB_PASSWORD_HANDSON
        self.db_host = DB_HOST_HANDSON

    def _conn(self):
        """Estabelece uma conexão com o banco de dados."""
        self.connection = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host
        )
        return self.connection

    def execute_query(self, query: str):
        """Executa uma query no banco de dados."""
        conn = self._conn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return "Query executada com sucesso"
        except Exception as e:
            conn.rollback()
            raise e
