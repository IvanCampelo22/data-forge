from db.register_update import EditRegisters
from db.company_db import InsertCompany
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def filter_date_service(table_name: str, schema_name: str, start_date: str, end_date: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Lógica para a filtragem de registros por range de data com paginação.
    
    Args: 
        table_name: Nome da tabela a ser consultada.
        schema_name: Nome do schema da tabela.
        start_date: Data inicial do range.
        end_date: Data final do range.
        limit: Número máximo de registros retornados por página (padrão: 10).
        offset: Número de registros a serem ignorados antes de começar a busca (padrão: 0).
    
    Returns:
        Um dicionário contendo os registros da página atual e informações de paginação.
    
    Raises:
        Exception: Caso ocorra um erro na consulta.
    """
    try:
        filter_by_data = InsertCompany()
        result = filter_by_data._get_news_by_date_range(table_name, schema_name, start_date, end_date, limit, offset)

        logger.info(f"Tabela '{table_name}' consultada com sucesso. Registros retornados: {len(result.get('data', []))}")
        return result
    except Exception as e:
        logger.error(f"Erro ao consultar tabela '{table_name}': {e}")
        return {"error": f"Erro ao consultar tabela: {str(e)}"}
    
def filter_trash_service(table_name: str, schema_name: str, start_date: str, end_date: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Lógica para a filtragem de registros deletados por range de data dentro da lixeira, com suporte a paginação.

    Args: 
        table_name (str): Nome da tabela a ser consultada.
        schema_name (str): Nome do schema no banco de dados.
        start_date (str): Data inicial do range no formato 'YYYY-MM-DD'.
        end_date (str): Data final do range no formato 'YYYY-MM-DD'.
        limit (int): Número máximo de registros retornados por página.
        offset (int): Número de registros a serem ignorados antes de retornar os resultados.

    Returns:
        Um dicionário contendo os registros deletados paginados e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        filter_by_data = EditRegisters()
        result = filter_by_data._get_deleted_records_by_date_range(table_name, schema_name, start_date, end_date, limit, offset)

        if not result or not result.get("deleted_records"):
            return {"message": "Nenhum registro deletado encontrado no período informado.", "total_records": 0}

        logger.info(f"Consulta na lixeira da tabela '{table_name}' realizada com sucesso. Registros retornados: {len(result.get('deleted_records', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar a lixeira da tabela '{table_name}': {e}")
        return {"error": f"Erro ao consultar registros deletados: {str(e)}"}
    
def filter_company_service(company_id: str, schema_name: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Lista tabelas associadas a uma empresa pelo identificador, com suporte a paginação.

    Args:
        company_id (str): Identificador da empresa no banco de dados.
        schema_name (str): Nome do schema no banco de dados.
        limit (int): Número máximo de tabelas a serem retornadas por página (padrão: 10).
        offset (int): Número de tabelas a serem ignoradas antes de começar a busca (padrão: 0).

    Returns:
        Um dicionário contendo as tabelas associadas paginadas e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        filter_by_data = InsertCompany()
        result = filter_by_data._find_tables_with_company_id(company_id, schema_name, limit, offset)

        logger.info(f"Tabelas associadas à empresa '{company_id}' consultadas com sucesso. Total de tabelas retornadas: {len(result.get('tables', []))}")
        return result
    except Exception as e:
        logger.error(f"Erro ao consultar tabelas associadas à empresa '{company_id}': {e}")
        return {"error": f"Erro ao consultar tabelas: {str(e)}"}