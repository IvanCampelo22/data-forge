from db.developer.developer_db import DeveloperDb
from db.manager.manager_db import ManagerDb
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_company_service() -> str:
    """
    Faz a migração da tabela de company no banco de dados
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:

        create_table_db = DeveloperDb()
        result = create_table_db._create_company_table()
        
        return result
    
    except Exception as e:
        return f"Erro ao criar tabela: {str(e)}"