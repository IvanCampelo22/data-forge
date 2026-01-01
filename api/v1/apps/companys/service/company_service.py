from db.register_update import EditRegisters
from db.company_db import InsertCompany
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_company_service(name_company: str, cnpj_company: str, email_company: str) -> List[Dict[Any, Any]]:
    """
    Registra empresa no banco de dados
    Args: 
        name_company (str): nome da empresa a ser criada
        cnpj_company (str): cnpj da empresa a ser criada
        email_company (str): email da empresa a ser criada
    Exception: 
        Retorna erros internos relacionados a conexão com o banco ou outras variáveis.
    """
    try:
        create_company_db = InsertCompany()
        result = create_company_db._create_company(name_company, cnpj_company, email_company)
        
        logger.info(f"Empresa '{name_company}' criada com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar empresa' {name_company}': {e}")
        return f"Erro ao criar empresa: {str(e)}"
    
def trash_register_service(
    table_name: str, schema_name: str, limit: int = 10, offset: int = 0
) -> Dict[str, Any]:
    """
    Lixeira que armazena os registros deletados pelo usuário, com suporte a paginação.

    Args:
        table_name (str): Nome da tabela a ser consultada.
        schema_name (str): Nome do schema da tabela.
        limit (int): Número máximo de registros retornados por página.
        offset (int): Número de registros a serem ignorados antes de retornar os resultados.

    Returns:
        Um dicionário contendo os registros deletados paginados e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        trash_db = EditRegisters()

        if not table_name:
            raise ValueError("Insira um valor correto para o nome da tabela.")

        # Chama a função que consulta registros deletados com paginação
        result = trash_db._get_deleted_records(table_name, schema_name, limit, offset)

        if not result or not result.get("trash"):
            return {"message": "Não há dados na lixeira.", "total_records": 0}

        logger.info(f"Consulta na tabela '{table_name}' realizada com sucesso. Registros retornados: {len(result.get('trash', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar a tabela '{table_name}': {e}")
        return {"error": f"Erro ao consultar tabela: {str(e)}"}
    
def get_records_service(table_name: str, schema_name: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Traz todos os registros ativos da tabela com suporte a paginação.

    Args:
        table_name (str): Nome da tabela a ser consultada.
        schema_name (str): Nome do schema da tabela.
        limit (int): Número máximo de registros retornados por página (padrão: 10).
        offset (int): Número de registros a serem ignorados antes de começar a busca (padrão: 0).

    Returns:
        Um dicionário contendo os registros ativos paginados e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        records = EditRegisters()
        result = records._get_active_record(table_name, schema_name, limit, offset)

        if not result or not result.get("active_records"):
            return {"message": "Nenhum registro ativo encontrado.", "total_records": 0}

        logger.info(f"Consulta na tabela '{table_name}' realizada com sucesso. Registros retornados: {len(result.get('active_records', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar tabela '{table_name}': {e}")
        return {"error": f"Erro ao consultar tabela: {str(e)}"}

def add_table_in_company_service(table_name: str, schema_name: str, company_id) -> str:
    """
    Associa uma empresa a uma tabela
    Args:
        table_name (str): nome da tabela a ser consultada.
        company_id: identificador da empresa no banco
    Exception:
        Ocorre quando acontecer algum problema com a consulta ao banco de dados.
    """
    try:
        associate_table_with_company_db = InsertCompany()
        result = associate_table_with_company_db._associate_table_with_company(table_name, schema_name, company_id)
        
        logger.info(f"Tabela '{table_name}' associada à empresa {company_id} com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao associar tabela' {table_name} à empresa {company_id}': {e}")
        return f"Erro ao criar empresa: {str(e)}"
    
def get_company_service(limit: int = 10, offset: int = 0) -> Dict[str, Any]:
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
        get_company_db = InsertCompany()
        result = get_company_db._get_all_companies(limit, offset)

        if not result or not result.get("companies"):
            return {"message": "Nenhuma empresa ativa encontrada.", "total_records": 0}

        logger.info(f"Consulta de empresas realizada com sucesso. Registros retornados: {len(result.get('companies', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar empresas: {e}")
        return {"error": f"Erro ao consultar empresas: {str(e)}"}
    
def trash_company_service(limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Busca por todas as empresas desativadas com suporte a paginação.

    Args:
        limit (int): Número máximo de empresas retornadas por página.
        offset (int): Número de registros a serem ignorados antes de retornar os resultados.

    Returns:
        Um dicionário contendo as empresas inativas paginadas e informações de paginação.

    Raises:
        Exception: Caso ocorra um erro na consulta ao banco de dados.
    """
    try:
        get_company_db = InsertCompany()
        result = get_company_db._trash_companies(limit, offset)

        if not result or not result.get("inactive_companies"):
            return {"message": "Nenhuma empresa inativa encontrada.", "total_records": 0}

        logger.info(f"Consulta de empresas inativas realizada com sucesso. Registros retornados: {len(result.get('inactive_companies', []))}")

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar lixeira: {e}")
        return {"error": f"Erro ao consultar lixeira: {str(e)}"}

def deactive_company_service(record_id: str) -> str:
    """
    Busca por todas as empresas desativadas
    Args:
        record_id (str): Identificador da empresa a ser desativada
    Exception:
        Caso haja alguma irregularidade no banco, vai retornar um exception
    """
    try:
        delete_company_db = InsertCompany()
        result = delete_company_db._mark_company_as_deleted_by_id(record_id)

        return result

    except Exception as e:
        logger.error(f"Erro ao consultar lixeira: {e}")
        return f"Erro ao consultar lixeira: {str(e)}"  

def active_company_service(record_id: str) -> str:
    """
    Remove empresas da lixeira, ativando elas novamente
    Args:
        record_id (str): Identificador da empresa a ser desativada
    Exception:
        Caso haja alguma irregularidade no banco, vai retornar um exception
    """
    try:
        get_company = InsertCompany()
        result = get_company._active_company(record_id)

        logger.info(f"Valores retornados com sucesso: {result}")
        return result

    except Exception as e:
        logger.error(f"Erro ao consultar lixeira: {e}")
        return f"Erro ao consultar lixeira: {str(e)}"
    
def update_company_service(company_id: str, name_company: str = None, cnpj_company: str = None, email_company: str = None) -> str:
    """
    Atualiza empresas com base no identificador
    Args: 
        name_company (str): nome da empresa a ser atualizada
        cnpj_company (str): cnpj da empresa a ser atualizada
        email_company (str): email da empresa a ser atualizada
    Exception: 
        Retorna erros internos relacionados a conexão com o banco ou outras variáveis.
    """
    try:
        update_company = InsertCompany()
        result = update_company._update_company(company_id, name_company, cnpj_company, email_company)

        logger.info(f"Empresa atualizada com sucesso: {result}")
        return result

    except Exception as e:
        logger.error(f"Erro ao consultar lixeira: {e}")
        return f"Erro ao consultar lixeira: {str(e)}"