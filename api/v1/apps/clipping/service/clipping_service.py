from db.clipping_db.get_table_news import GetNews
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)    

def get_active_company_clipping(limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Busca por todas as empresas ativas no clipping com suporte a paginação.

    Args:
        limit (int): Número máximo de empresas retornadas por página.
        offset (int): Número de registros a serem ignorados antes de retornar os resultados.

    Returns:
        Um dicionário contendo as empresas ativas paginadas e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        get_company = GetNews()
        result = get_company._get_active_companies(limit, offset)

        if not result or not result.get("active_companies"):
            return {"message": "Nenhuma empresa ativa encontrada.", "total_records": 0}

        logger.info(f"Consulta de empresas ativas no clipping realizada com sucesso. Registros retornados: {len(result.get('active_companies', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar empresas ativas no clipping: {e}")
        return {"error": f"Erro ao consultar empresas ativas no clipping: {str(e)}"}
    
def get_deactivate_company_clipping(limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Busca por todas as empresas desativadas no clipping com suporte a paginação.

    Args:
        limit (int): Número máximo de empresas retornadas por página.
        offset (int): Número de registros a serem ignorados antes de retornar os resultados.

    Returns:
        Um dicionário contendo as empresas desativadas paginadas e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        get_company = GetNews()
        result = get_company._get_deactivate_companies(limit, offset)

        if not result or not result.get("deactivated_companies"):
            return {"message": "Nenhuma empresa desativada encontrada.", "total_records": 0}

        logger.info(f"Consulta de empresas desativadas no clipping realizada com sucesso. Registros retornados: {len(result.get('deactivated_companies', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar empresas desativadas no clipping: {e}")
        return {"error": f"Erro ao consultar empresas desativadas no clipping: {str(e)}"}
    
def save_news_service(news_data) -> Any:
    """
    Salva dados do upload do arquivo no banco de dados
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:
        news_db = GetNews()
        result = news_db._save_news(news_data=news_data)

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar empresas: {e}")
        raise Exception(f"Erro ao consultar empresas: {str(e)}")
    
def transfer_active_company_to_handson(company_name: str) -> str:
    """
    Busca uma empresa ativa pelo nome na tabela `company_company` e,
    se existir, transfere seus dados para `handson.company`.

    Args:
        company_name (str): Nome da empresa.

    Returns:
        str: Mensagem indicando se a empresa foi inserida ou já existia.

    Exception:
        Trata erros de conexão com o banco de dados ou outras variáveis.
    """
    try:
        get_company = GetNews()  # Classe responsável por interagir com o banco
        company_id = get_company._transfer_company_to_handson(company_name)

        return f"Empresa '{company_name}' foi transferida para o Hands-On com ID {company_id}."

    except Exception as e:
        logger.error(f"Erro ao transferir empresa '{company_name}': {e}")
        raise Exception(f"Erro ao transferir empresa '{company_name}': {str(e)}")