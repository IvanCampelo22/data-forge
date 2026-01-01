from db.insert_column import InsertColumn
from db.register_update import EditRegisters
from db.manager.manager_db import ManagerDb
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delete_table_service(table_name: str, schema_name: str) -> str:
    """
    Deleta tabela no banco de dados
    Args:
        table_name (str): Nome da tabela que vai ser criada
        schema_name (str): Nome do esquema onde está a tabela
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:

        create_table_db = ManagerDb()
        result = create_table_db._delete_table(table_name, schema_name)
        
        logger.info(f"Tabela '{table_name}' deleta com sucesso.")
        return result
    
    except Exception as e:
        logger.error(f"Erro ao deletar tabela '{table_name}': {e}")
        return f"Erro ao deletar tabela: {str(e)}"
    

def delete_schema_service(schema_name: str) -> str:
    """
    Deleta schema no banco de dados
    Args:
        schema_name (str): Nome do esquema onde está a tabela
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:

        create_table_db = ManagerDb()
        result = create_table_db._delete_schema(schema_name)
        
        logger.info(f"Schema '{schema_name}' deleta com sucesso.")
        return result
    
    except Exception as e:
        logger.error(f"Erro ao deletar schema '{schema_name}': {e}")
        return f"Erro ao deletar schema: {str(e)}"
    

def delete_column_service(table_name: str, schema_name: str, column_name: str) -> str:
    """
    Deleta colunas em uma tabela no banco de dados
    Args:
        table_name (str): Nome da tabela onde a coluna vai ser localizada
        schema_name (str): Nome do esquema onde está a tabela
        column_name (str): Nome da coluna a ser deletada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:

        create_column_db = ManagerDb()
        result = create_column_db._delete_column(table_name, schema_name, column_name)
        
        logger.info(f"Coluna '{column_name}' deleta com sucesso.")
        return result
    
    except Exception as e:
        logger.error(f"Erro ao deletar tabela '{column_name}': {e}")
        return f"Erro ao deletar tabela: {str(e)}"
    