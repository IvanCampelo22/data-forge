from db.insert_column import InsertColumn
from db.register_update import EditRegisters
from db.create_tables import CreateInDb
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_table_service(table_name: str, schema_name: str) -> str:
    """
    Cria tabela no banco de dados
    Args:
        table_name (str): Nome da tabela que vai ser criada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:

        create_table_db = CreateInDb()
        result = create_table_db._create_table(table_name, schema_name)
        
        logger.info(f"Tabela '{table_name}' criada com sucesso.")
        return result
    
    except Exception as e:
        logger.error(f"Erro ao criar tabela '{table_name}': {e}")
        return f"Erro ao criar tabela: {str(e)}"
    
def create_schema_service(schema_name: str) -> str:
    """
    Cria um schema no banco de dados.

    Args:
        schema_name (str): Nome do schema que vai ser criado.

    Exception:
        Trata erros de conexão com o banco e outras variáveis.
    """
    try:
        create_schema_db = CreateInDb()
        result = create_schema_db._create_schema(schema_name)

        logger.info(f"Schema '{schema_name}' criado com sucesso.")
        return result

    except Exception as e:
        logger.error(f"Erro ao criar schema '{schema_name}': {e}")
        return f"Erro ao criar schema: {str(e)}"
    
def create_column_text_service(table_name: str, schema_name: str,column_name: str) -> str:
    """
    Criar coluna de texto em uma tabela específica
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        column_name (str): Nome da coluna a ser criada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:
        create_column_db = InsertColumn()
        result = create_column_db._insert_column_text(table_name, schema_name, column_name)
        
        logger.info(f"Tabela '{column_name}' criada com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar tabela '{column_name}': {e}")
        return f"Erro ao criar tabela: {str(e)}"

def create_column_integer_service(db_name: str, schema_name: str, column_name: str) -> str:
    """
    Criar coluna de número inteiro em uma tabela específica
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        column_name (str): Nome da coluna a ser criada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:
        create_column_service = InsertColumn()   
        result = create_column_service._insert_column_integer_number(db_name, schema_name, column_name)
        
        logger.info(f"Tabela '{column_name}' criada com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar tabela '{column_name}': {e}")
        return f"Erro ao criar tabela: {str(e)}"
    
def create_column_float_service(db_name: str, schema_name, column_name: str) -> str:
    """
    Criar coluna de número float em uma tabela específica
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        column_name (str): Nome da coluna a ser criada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:
        create_column_db = InsertColumn()   
        result = create_column_db._insert_column_float_number(db_name, schema_name, column_name)
        
        logger.info(f"Coluna '{column_name}' criada com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar coluna '{column_name}': {e}")
        return f"Erro ao criar coluna: {str(e)}"
    
def create_column_date_service(db_name: str, schema_name: str, column_name: str) -> str:
    """
    Criar coluna de date em uma tabela específica
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        column_name (str): Nome da coluna a ser criada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:
        create_column_db = InsertColumn()   
        result = create_column_db._insert_column_date(db_name, schema_name, column_name)
        
        logger.info(f"Coluna '{column_name}' criada com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar coluna '{column_name}': {e}")
        return f"Erro ao criar coluna: {str(e)}"
    
def create_column_boolean_service(db_name: str, schema_name: str, column_name: str) -> str:
    """
    Criar coluna de boolean em uma tabela específica
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        column_name (str): Nome da coluna a ser criada
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try:
        create_column_dv = InsertColumn()   
        result = create_column_dv._insert_column_boolean(db_name, schema_name, column_name)
        
        logger.info(f"Coluna '{column_name}' criada com sucesso.")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar coluna '{column_name}': {e}")
        return f"Erro ao criar coluna: {str(e)}"
    
def update_field_name_service(table_name: str, schema_name: str, old_field_name: str, new_field_name: str):
    """
    Atualiza o nome de um campo
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        old_field_name (str): Nome antigo da coluna
        new_field_name (str): Novo nome da tabela
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try: 
       
        update_field_db = EditRegisters()
        result = update_field_db._rename_field_with_validation(table_name=table_name, schema_name=schema_name, old_field_name=old_field_name, new_field_name=new_field_name)

        return result
    except Exception as e:
        logger.error(f"Erro ao renomear campo da tabela: '{table_name}': {e}")
        return f"Erro ao criar coluna: {str(e)}"
    
def change_type_field_service(table_name, schema_name, field_name, field_type):
    """
    Atualiza o nome de um campo
    Args:
        table_name (str): Nome da tabela onde o novo campo vai ser inserido
        field_name (str): Nome do campo a ser atualizado
        field_type (str): Novo tipo do campo
    Exception:
        Trata erros de conexão com o banco e outras variáveis
    """
    try: 
        change_type_db = EditRegisters()
        result = change_type_db._change_field_type_with_validation(table_name, schema_name, field_name, field_type)
        return result
    except Exception as e:
        logger.error(f"Erro ao mudar tipo do campo: '{field_name}': {e}")
        return f"Erro ao mudar nome do campo: {str(e)}"
    
def delete_column_service(db_name: str, schema_name: str, record_id: str) -> List[Dict[Any, Any]]:
    """
    Lixeira que armazena os registros deletados pelo usuário
    Args: 
        table_name: nome da tabela a ser consultada
        record_id: identificador do registro que deve ser excluido
    Exception: 
        Retorna erros internos relacionados a conexão com o banco ou outras variáveis.
    """
    try:
        delete_column_db = EditRegisters()

        if not db_name: 
            raise ValueError("Insira o nome correto da tabela que deseja deletar")

        if not record_id: 
            raise ValueError("Insira um valor válido para o id da coluna")

        result = delete_column_db._mark_as_deleted_by_id(db_name, schema_name, record_id)

        logger.info(f"Tabela '{record_id}' delete realizado com sucesso.")
        return result
    
    except Exception as e:
        logger.error(f"Erro ao deletar dados ' {record_id}': {e}")
        return f"Erro ao criar tabela: {str(e)}"
    
def active_register_service(table_name: str, schema_name: str, record_id: str) -> List[Dict[Any, Any]]:
    """
    Reativa registros desativados
    Args: 
        table_name: nome da tabela a ser consultada
        record_id: identificador do registro que deve ser excluido
    Exception: 
        Retorna erros internos relacionados a conexão com o banco ou outras variáveis.
    """
    try:
        active_column_db = EditRegisters()

        if not table_name: 
            raise ValueError("Insira o nome correto da tabela que deseja deletar")

        if not record_id: 
            raise ValueError("Insira um valor válido para o id da coluna")

        result = active_column_db._active_register(table_name, schema_name, record_id)

        logger.info(f"Tabela '{record_id}' delete realizado com sucesso.")
        return result
    
    except Exception as e:
        logger.error(f"Erro ao deletar dados ' {record_id}': {e}")
        return f"Erro ao criar tabela: {str(e)}"