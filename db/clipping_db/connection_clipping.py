from config.config import DB_HOST_CLIPPING, DB_NAME_CLIPPING, DB_OPTIONS_CLIPPING, DB_PASSWORD_CLIPPING, DB_USER_CLIPPING
import psycopg2

class DatabaseClipping:

    def __init__(self):
        self.db_name = DB_NAME_CLIPPING
        self.db_user = DB_USER_CLIPPING
        self.db_password = DB_PASSWORD_CLIPPING
        self.db_host = DB_HOST_CLIPPING
        self.options = DB_OPTIONS_CLIPPING

    def _conn(self):
        """Estabelece uma conexão com o banco de dados."""
        self.connection = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            options=self.options
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
        finally:
            cursor.close()
            conn.close()
