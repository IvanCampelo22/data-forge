import psycopg2 
from db.connection import Database
from psycopg2 import sql


class CreateInDb(Database):

    def __init__(self) -> None:
        super().__init__()

    def _create_table(self, table_name: str, schema_name: str) -> str:
        """
        Cria uma tabela no banco de dados com os campos `uuid`, `created_at`, 
        e restrição de unicidade em `news_code` e `uuid`.

        Args:
            table_name (str): Nome da tabela a ser criada.
            schema_name (str): Nome do schema onde a tabela será criada.

        Returns:
            str: Confirmação da criação da tabela.

        Raises:
            Exception: Caso ocorra um erro na criação da tabela.
        """
        try:
            query = sql.SQL(
                """
                CREATE TABLE {}.{} (
                    id SERIAL PRIMARY KEY,
                    date DATE NULL,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    news_code VARCHAR NULL,
                    company_id INTEGER REFERENCES company(id) NULL,
                    UNIQUE(news_code, id) 
                );
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            )

            result = self.execute_query(query)
            
            # Adiciona a restrição de unicidade
            unique_constraint_query = sql.SQL(
                """
                ALTER TABLE {}.{} ADD CONSTRAINT unique_news UNIQUE (news_code, company_id, date);
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            )
            self.execute_query(unique_constraint_query)
            
            return f"Tabela {schema_name}.{table_name} criada com sucesso."

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar tabela: {e}")

        finally:
            self.connection.close()

    def _create_schema(self, schema_name: str) -> str:
        """
        Cria um schema no banco de dados.

        Args:
            schema_name (str): Nome do schema a ser criado.

        Exception:
            Trata erros de conexão ou outras variáveis.

        Finally:
            Fecha conexão com o banco de dados.
        """
        try:
            query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
                sql.Identifier(schema_name)
            )
            result = self.execute_query(query)
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar schema: {e}")
        finally:
            self.connection.close()
        
    