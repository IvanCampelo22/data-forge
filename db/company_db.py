from db.connection import Database
from typing import List, Dict, Any
from psycopg2 import sql
import pandas as pd

class InsertCompany(Database):

    def __init__(self) -> None:
        super().__init__()
        self.conn = self._conn()

    @property
    def conn_to_database(self):
        return self.conn

    def _create_company(self, name_company: str, cnpj_company: str, email_company: str) -> str:
        """
        Registra empresa no banco de dados
        Args: 
            name_company (str): nome da empresa a ser criada
            cnpj_company (str): cnpj da empresa a ser criada
            email_company (str): email da empresa a ser criada
        Exception: 
            Trata erros de conexão ou outras variáveis
        Finally: 
            Fecha conexão com o banco de dados
        """
        try:
            query = sql.SQL(
                """
                INSERT INTO company (name, cnpj, email_company, is_active) 
                VALUES ({name}, {cnpj}, {email}, TRUE)
                RETURNING id;
                """
            ).format(
                name=sql.Literal(name_company),
                cnpj=sql.Literal(cnpj_company),
                email=sql.Literal(email_company)
            )

            result = self.execute_query(query.as_string(self._conn()))
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()

    def _associate_table_with_company(self, table_name: str, company_id: int) -> str:
        """
        Atualiza o company_id de uma tabela
        Args:
            table_name (str): Nome da tabela a ser alterada
            company_id (id): Identificador da empresa
        Exception: 
            Trata erros de conexão ou outras variáveis
        Finally: 
            Fecha conexão com o banco de dados
        """
        
        try:
            query = sql.SQL(
                """
                UPDATE {table}
                SET company_id = {company_id}
                WHERE company_id IS NULL
                RETURNING id;
                """
            ).format(
                table=sql.Identifier(table_name),  
                company_id=sql.Literal(company_id)
            )

            result = self.execute_query(query.as_string(self._conn()))
            return result
        
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()

    def _get_all_companies(self, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Retorna empresas onde is_active é True, com suporte a paginação.

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
                SELECT *
                FROM company
                WHERE is_active = TRUE
                ORDER BY id ASC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de empresas ativas
            count_query = """
                SELECT COUNT(*)
                FROM company
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
                "companies": result
            }

        except Exception as e:
            self.conn_to_database.rollback()
            raise Exception(f"Erro ao consultar empresas ativas: {str(e)}")

        finally:
            if self.conn_to_database:
                self.conn_to_database.close()
    
    def _trash_companies(self, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Retorna empresas onde is_active é False, com suporte a paginação.

        Args:
            limit (int): Número máximo de empresas retornadas por página.
            offset (int): Número de registros a serem ignorados antes de retornar os resultados.

        Returns:
            Um dicionário contendo as empresas inativas paginadas e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro na consulta ao banco de dados.
        """
        try:
            # Query para buscar empresas inativas com paginação
            query = """
                SELECT *
                FROM company
                WHERE is_active = FALSE
                ORDER BY id ASC
                LIMIT %s OFFSET %s;
            """

            # Query para contar o total de empresas inativas
            count_query = """
                SELECT COUNT(*)
                FROM company
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
                "inactive_companies": result
            }

        except Exception as e:
            self.conn_to_database.rollback()
            raise Exception(f"Erro ao consultar empresas inativas: {str(e)}")

        finally:
            if self.conn_to_database:
                self.conn_to_database.close()
        
    def _mark_company_as_deleted_by_id(self, record_id: int) -> str:
        """
        Desativa a empresa por meio do parametro que é o identificador.
        Args:
            record_id (str): identificador da
        Exception: 
            Trata erros de conexão ou outras variáveis
        Finally: 
            Fecha conexão com o banco de dados
        """
        try:
            query = f"""
                UPDATE company
                SET is_active = FALSE
                WHERE id = {record_id};
            """
            result = self.execute_query(query) 
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()
    
    def _active_company(self, record_id: int) -> Dict[str, str]:
        """
        Ativa a empresa por meio do parametro que é o identificador.
        Args:
            record_id (str): identificador da
        Exception: 
            Trata erros de conexão ou outras variáveis
        Finally: 
            Fecha conexão com o banco de dados
        """
        try:
            query = f"""
                UPDATE company
                SET is_active = TRUE
                WHERE id = {record_id};
            """
            result = self.execute_query(query) 
            return result
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Erro ao criar coluna: {e}")
        finally:
            self.connection.close()
    
    # FIXME verificar a possível criação de um campo padrão para a consulta no range de data
    def _get_news_by_date_range(self, table_name: str, schema_name: str, start_date: str, end_date: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Filtra registros pelo range de data com paginação.
        
        Args: 
            table_name: Nome da tabela a ser consultada.
            schema_name: Nome do schema da tabela.
            start_date: Data inicial para o range de data.
            end_date: Data final para o range de data.
            limit: Número máximo de registros retornados por página.
            offset: Número de registros a serem ignorados antes de começar a retornar os resultados.

        Returns:
            Um dicionário contendo os registros da página atual e informações de paginação.
            
        Raises:
            Exception: Erro ao consultar notícias.
        """
        try:
            query = f"""
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE is_deleted = FALSE
                AND date BETWEEN %s AND %s
                ORDER BY date DESC
                LIMIT %s OFFSET %s;
            """

            count_query = f"""
                SELECT COUNT(*)
                FROM {schema_name}.{table_name}
                WHERE is_deleted = FALSE
                AND date BETWEEN %s AND %s;
            """

            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query, (start_date, end_date, limit, offset))
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                cursor.execute(count_query, (start_date, end_date))
                total_records = cursor.fetchone()[0]

                if not rows:
                    return {"message": "Nenhum registro encontrado para o período informado.", "total_records": total_records}

                result = [dict(zip(columns, row)) for row in rows]

                return {
                    "total_records": total_records,
                    "page_size": limit,
                    "current_offset": offset,
                    "data": result
                }
        except Exception as e:
            raise Exception(f"Erro ao consultar notícias: {str(e)}")
        finally:
            if self.conn:
                self.conn.close()

    def _find_tables_with_company_id(self, company_id: int, schema_name: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Query responsável por filtrar tabelas pelo ID da empresa e retornar seus conteúdos com paginação.

        Args: 
            company_id (int): Identificador da empresa.
            schema_name (str): Nome do schema.
            limit (int): Número máximo de tabelas a serem retornadas por página.
            offset (int): Número de tabelas a serem ignoradas antes de começar a busca.

        Returns:
            Um dicionário contendo as tabelas relacionadas paginadas e informações de paginação.

        Raises:
            Exception: Caso ocorra um erro durante a consulta.
        """
        try:
            # Consulta para obter todas as tabelas que possuem a coluna 'company_id' no schema
            query_tables = """
                SELECT table_name
                FROM information_schema.columns
                WHERE column_name = 'company_id'
                AND table_schema = %s
                GROUP BY table_name
                ORDER BY table_name
                LIMIT %s OFFSET %s;
            """

            with self.conn_to_database.cursor() as cursor:
                cursor.execute(query_tables, (schema_name, limit, offset))
                tables = [row[0] for row in cursor.fetchall()]

            # Consulta para contar o total de tabelas sem paginação
            count_query = """
                SELECT COUNT(DISTINCT table_name)
                FROM information_schema.columns
                WHERE column_name = 'company_id'
                AND table_schema = %s;
            """

            with self.conn_to_database.cursor() as cursor:
                cursor.execute(count_query, (schema_name,))
                total_tables = cursor.fetchone()[0]

            related_tables_with_data = []
            with self.conn_to_database.cursor() as cursor:
                for table in tables:
                    query_check = f"""
                        SELECT * FROM {schema_name}.{table} WHERE company_id = %s LIMIT 1;
                    """
                    cursor.execute(query_check, (company_id,))
                    rows = cursor.fetchall()

                    if rows:
                        column_names_and_types_query = """
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = %s
                            AND table_schema = %s;
                        """
                        cursor.execute(column_names_and_types_query, (table, schema_name))
                        columns_with_types = [
                            {"name": row[0], "type": row[1]} for row in cursor.fetchall()
                        ]

                        related_tables_with_data.append({
                            "table_name": table,
                            "columns": columns_with_types
                        })

            return {
                "total_tables": total_tables,
                "page_size": limit,
                "current_offset": offset,
                "tables": related_tables_with_data
            }
        except Exception as e:
            raise Exception(f"Erro ao buscar tabelas relacionadas ao company_id: {str(e)}")
        finally:
            if self.conn_to_database:
                self.conn_to_database.close()


    def _export_data_to_excel(self, start_date: str, end_date: str) -> str:
        try:
            query = """
                SELECT * 
                FROM braskem
                WHERE date_column BETWEEN %s AND %s
            """

            result, columns = self.execute_query(query, (start_date, end_date))
            df = pd.DataFrame(result, columns=columns)
            file_path = f"/Users/ivancampelo/Projects/Charisma/handson/exported_data_{start_date.replace('-', '')}_to_{end_date.replace('-', '')}.xlsx"
            df.to_excel(file_path, index=False)

            return file_path
        
        except Exception as e:
            raise Exception(f"Erro ao buscar tabelas relacionadas ao company_id: {str(e)}")
        finally:
            if self.conn_to_database:
                self.conn_to_database.close()

    def _update_company(self, company_id: int, name_company: str = None, cnpj_company: str = None, email_company: str = None) -> str:
        """
        Atualiza empresas com base no identificador
        Args: 
            name_company (str): nome da empresa a ser atualizada
            cnpj_company (str): cnpj da empresa a ser atualizada
            email_company (str): email da empresa a ser atualizada
        Exception: 
            Trata erros de conexão ou outras variáveis
        Finally: 
            Fecha conexão com o banco de dados
        """
        try:
            updates = []
            if name_company:
                updates.append(sql.SQL("name = {name}").format(name=sql.Literal(name_company)))
            if cnpj_company:
                updates.append(sql.SQL("cnpj = {cnpj}").format(cnpj=sql.Literal(cnpj_company)))
            if email_company:
                updates.append(sql.SQL("email_company = {email}").format(email=sql.Literal(email_company)))
            
            if not updates:
                raise ValueError("Nenhum campo para atualizar foi fornecido.")

            query = sql.SQL(
                """
                UPDATE company
                SET {fields}
                WHERE id = {company_id}
                RETURNING id;
                """
            ).format(
                fields=sql.SQL(", ").join(updates),
                company_id=sql.Literal(company_id)
            )
            
            result = self.execute_query(query.as_string(self._conn()))
            return result
        except Exception as e:
            raise Exception(f"Erro ao atualizar empresa: {str(e)}")
        finally:
            if self.conn_to_database:
                self.conn_to_database.close()

    