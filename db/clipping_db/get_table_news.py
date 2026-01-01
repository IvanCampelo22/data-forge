from db.clipping_db.connection_clipping import DatabaseClipping
from config.config import DB_HOST_CLIPPING, DB_NAME_CLIPPING, DB_OPTIONS_CLIPPING, DB_PASSWORD_CLIPPING, DB_USER_CLIPPING
from config.config import DB_HOST_HANDSON, DB_NAME_HANDSON, DB_PASSWORD_HANDSON, DB_USER_HANDSON
import psycopg2
from typing import List, Dict, Any

class GetNews(DatabaseClipping):

    def __init__(self) -> None:
        super().__init__()
        self.conn = self._conn()

    @property
    def conn_to_database(self):
        return self.conn
    

    def _get_active_companies(self, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Busca por todas as empresas ativas com suporte a paginação.

        Args:
            limit (int): Número máximo de empresas retornadas por página.
            offset (int): Número de registros a serem ignorados antes de retornar os resultados.

        Returns:
            Um dicionário contendo as empresas ativas paginadas e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro na consulta ao banco de dados.
        """
        try:
            # Query para buscar empresas ativas com paginação
            query = """
                SELECT * FROM company_company
                WHERE is_active = TRUE
                ORDER BY id ASC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de empresas ativas
            count_query = """
                SELECT COUNT(*) FROM company_company
                WHERE is_active = TRUE;
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
                "active_companies": result
            }

        except Exception as e:
            raise Exception(f"Erro ao consultar empresas ativas: {str(e)}")

        finally:
            if self.conn_to_database:
                self.conn_to_database.close()

    def _deactivate_clipping_news(self, news_code: str) -> None:
        """
        Desativa uma notícia no clipping alterando `is_active` para `FALSE`.
        
        Args:
            news_code (str): Código da notícia que será desativada.
        
        Exception:
            Trata erros de conexão com o banco de dados ou outras variáveis.
        
        Finally:
            Fecha a conexão com o banco de dados.
        """
        try:
            query = """
                UPDATE news_charisma.clippings_news
                SET is_active = FALSE
                WHERE news_code = %s;
            """
            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query, (news_code,))
                self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao desativar a notícia {news_code}: {str(e)}")

        finally:
            if self.connection:
                self.connection.close()

    def _get_deactivate_companies(self, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Busca por todas as empresas desativadas com suporte a paginação.

        Args:
            limit (int): Número máximo de empresas retornadas por página.
            offset (int): Número de registros a serem ignorados antes de retornar os resultados.

        Returns:
            Um dicionário contendo as empresas desativadas paginadas e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro na consulta ao banco de dados.
        """
        try:
            # Query para buscar empresas desativadas com paginação
            query = """
                SELECT * FROM company_company
                WHERE is_active = FALSE
                ORDER BY id ASC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de empresas desativadas
            count_query = """
                SELECT COUNT(*) FROM company_company
                WHERE is_active = FALSE;
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
                "deactivated_companies": result
            }

        except Exception as e:
            raise Exception(f"Erro ao consultar empresas desativadas: {str(e)}")

        finally:
            if self.conn_to_database:
                self.conn_to_database.close()

    def _active_clipping_news(self, news_code: str) -> None:
        """
        Desativa uma notícia no clipping alterando `is_active` para `FALSE`.
        
        Args:
            news_code (str): Código da notícia que será desativada.
        
        Exception:
            Trata erros de conexão com o banco de dados ou outras variáveis.
        
        Finally:
            Fecha a conexão com o banco de dados.
        """
        try:
            query = """
                UPDATE news_charisma.clippings_news
                SET is_active = TRUE
                WHERE news_code = %s;
            """
            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query, (news_code,))
                self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao ativar a notícia {news_code}: {str(e)}")

        finally:
            if self.connection:
                self.connection.close()

    def _save_news(self, news_data):
        """
        Salva os dados na tabela News.
        Args: 
            news_data: Dados que serão salvos na tabela de notícias do clipping
        Exception: 
            Trata erros de conexão ou outras variáveis
        Finally: 
            Fecha conexão com o banco de dados
        """
        
        try:
            query = """
                INSERT INTO clippings_news (
                    publication_date, vehicle, title, theme, subject_name_slug,
                    media_type, tier, feeling, readers, journalist, original_link,
                    valuation, created_date, modified_date, is_active, approved_news, company_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            values = (
                news_data["publication_date"],
                news_data["vehicle"],
                news_data["title"],
                news_data["theme"],
                news_data["subject_name_slug"],
                news_data["media_type"],
                news_data["tier"],
                news_data["feeling"],
                news_data["readers"],
                news_data["journalist"],
                news_data["original_link"],
                news_data["valuation"],
                news_data["created_date"],
                news_data["modified_date"],
                news_data["is_active"],
                news_data["approved_news"],
                news_data["company_id"]
            )
            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query, values)
                news_id = cursor.fetchone()[0]
                self.connection.commit()
            return news_id
        except Exception as e:
            raise Exception(f"Erro ao salvar notícia: {str(e)}")
        finally:
            if self.connection:
                self.connection.close()
        
    
    def _transfer_company_to_handson(self, company_name: str) -> int:
        """
        Busca a empresa ativa pelo nome na tabela `company_company` no banco CLIPPING e,
        se existir, salva seus dados (incluindo o ID original) na tabela `company` no banco HANDSON.

        Args:
            company_name (str): Nome da empresa.

        Returns:
            int: ID da empresa no Hands-On (se encontrada e inserida).

        Exception:
            Trata erros de conexão com o banco de dados ou outras variáveis.

        Finally:
            Fecha as conexões com os bancos.
        """
        connection_clipping = None
        connection_handson = None
        cursor_clipping = None
        cursor_handson = None

        try:
            # Conectar ao banco Clipping (para buscar a empresa)
            connection_clipping = psycopg2.connect(
                dbname=DB_NAME_CLIPPING,
                user=DB_USER_CLIPPING,
                password=DB_PASSWORD_CLIPPING,
                host=DB_HOST_CLIPPING,
                options=DB_OPTIONS_CLIPPING
            )
            cursor_clipping = connection_clipping.cursor()

            # 🔍 Buscar a empresa ativa no banco Clipping
            query_clipping = """
                SELECT id, corporate_name, cnpj, email
                FROM company_company 
                WHERE is_active = TRUE 
                AND LOWER(TRIM(corporate_name)) = LOWER(TRIM(%s));
            """
            cursor_clipping.execute(query_clipping, (company_name,))
            company_data = cursor_clipping.fetchone()

            if not company_data:
                raise Exception(f"Empresa '{company_name}' não encontrada no Clipping ou está inativa.")

            company_id, corporate_name, cnpj, email = company_data  # Pegamos o ID original

            # Conectar ao banco Hands-On (para inserir a empresa)
            connection_handson = psycopg2.connect(
                dbname=DB_NAME_HANDSON,
                user=DB_USER_HANDSON,
                password=DB_PASSWORD_HANDSON,
                host=DB_HOST_HANDSON
            )
            cursor_handson = connection_handson.cursor()

            # 🔹 **Definir o schema correto**
            SCHEMA_HANDSON = "public"  # 🔹 Ajuste conforme necessário
            cursor_handson.execute(f"SET search_path TO {SCHEMA_HANDSON};")

            # 🔍 Verificar se a empresa já existe no Hands-On
            query_check = f"""
                SELECT id FROM {SCHEMA_HANDSON}.company 
                WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s));
            """
            cursor_handson.execute(query_check, (corporate_name,))
            existing_company = cursor_handson.fetchone()

            if existing_company:
                return existing_company[0]  # Retorna o ID da empresa já existente

            # 🟢 Inserir empresa no Hands-On com `OVERRIDING SYSTEM VALUE`
            query_insert = f"""
                INSERT INTO {SCHEMA_HANDSON}.company (id, name, cnpj, email_company, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (%s, %s, %s, %s, TRUE)
                RETURNING id;
            """
            cursor_handson.execute(query_insert, (company_id, corporate_name, cnpj, email))
            new_company_id = cursor_handson.fetchone()[0]

            # Commit na transação do Hands-On
            connection_handson.commit()
            return new_company_id

        except Exception as e:
            if connection_handson:
                connection_handson.rollback()
            raise Exception(f"Erro ao transferir empresa '{company_name}': {str(e)}")

        finally:
            # Fechar cursores e conexões
            if cursor_clipping:
                cursor_clipping.close()
            if connection_clipping:
                connection_clipping.close()
            if cursor_handson:
                cursor_handson.close()
            if connection_handson:
                connection_handson.close()