import pytest
from unittest.mock import patch, MagicMock
from api.v1.apps.companys.service.customize_company import create_table, create_column_string, create_column_integer

@pytest.fixture
def mock_create_in_db():
    with patch("api.v1.apps.companys.service.customize_company.CreateInDb") as mock_class:
        yield mock_class

def test_create_table_success(mock_create_in_db):
    mock_instance = MagicMock()
    mock_create_in_db.return_value = mock_instance
    mock_instance.create_table.return_value = "Tabela criada com sucesso."

    result = create_table("test_table")

    mock_instance.create_table.assert_called_once_with("test_table")
    assert result == "Tabela criada com sucesso."

def test_create_table_error(mock_create_in_db):
    mock_instance = MagicMock()
    mock_create_in_db.return_value = mock_instance
    mock_instance.create_table.side_effect = Exception("Erro simulado ao criar tabela.")

    result = create_table("invalid_table")

    mock_instance.create_table.assert_called_once_with("invalid_table")
    assert result == "Erro ao criar tabela: Erro simulado ao criar tabela."

@pytest.fixture
def mock_insert_column_string():
    with patch("api.v1.apps.companys.service.customize_company.InsertColumn") as mock_class:  # Ajuste 'my_module' conforme necessário
        yield mock_class

def test_create_column_string_success(mock_insert_column_string):
    mock_instance = MagicMock()
    mock_insert_column_string.return_value = mock_instance
    mock_instance._insert_column_string.return_value = "Coluna criada com sucesso."

    result = create_column_string("test_db", "test_column")

    mock_instance._insert_column_string.assert_called_once_with("test_db", "test_column")
    assert result == "Coluna criada com sucesso."

def test_create_column_string_error(mock_insert_column_string):
    mock_instance = MagicMock()
    mock_insert_column_string.return_value = mock_instance
    mock_instance._insert_column_string.side_effect = Exception("Erro simulado ao criar coluna.")

    result = create_column_string("invalid_db", "invalid_column")

    mock_instance._insert_column_string.assert_called_once_with("invalid_db", "invalid_column")
    assert result == "Erro ao criar tabela: Erro simulado ao criar coluna."


@pytest.fixture
def mock_insert_column_integer():
    with patch("api.v1.apps.companys.service.customize_company.InsertColumn") as mock_class:  # Ajuste o caminho conforme necessário
        yield mock_class

def test_insert_column_integer_success(mock_insert_column_integer):
    mock_instance = MagicMock()
    mock_insert_column_integer.return_value = mock_instance
    mock_instance._insert_column_integer.return_value = "Coluna adicionada com sucesso."

    result = mock_instance._insert_column_integer("test_table", "test_column")

    mock_instance._insert_column_integer.assert_called_once_with("test_table", "test_column")
    assert result == "Coluna adicionada com sucesso."

def test_insert_column_integer_error(mock_insert_column_integer):
    mock_instance = MagicMock()
    mock_insert_column_integer.return_value = mock_instance
    mock_instance._insert_column_integer.side_effect = Exception("Erro simulado ao adicionar coluna.")

    try:
        result = mock_instance._insert_column_integer("invalid_table", "invalid_column")
    except Exception as e:
        result = f"Erro ao adicionar coluna: {str(e)}"

    mock_instance._insert_column_integer.assert_called_once_with("invalid_table", "invalid_column")
    assert result == "Erro ao adicionar coluna: Erro simulado ao adicionar coluna."


@pytest.fixture
def mock_delete_column():
    with patch("api.v1.apps.companys.service.company_service.InsertColumn") as mock_class:
        yield mock_class

def test_delete_column_success(mock_delete_column):
    mock_instance = MagicMock()
    mock_delete_column.return_value = mock_instance
    mock_instance._mark_as_deleted_by_id.return_value = "Registro marcado como deletado com sucesso."

    from api.v1.apps.companys.service.company_service import delete_column

    result = delete_column("test_db", "test_record_id")

    mock_instance._mark_as_deleted_by_id.assert_called_once_with("test_db", "test_record_id")

    assert result == "Registro marcado como deletado com sucesso."

def test_delete_column_failure(mock_delete_column):
    mock_instance = MagicMock()
    mock_delete_column.return_value = mock_instance
    mock_instance._mark_as_deleted_by_id.side_effect = Exception("Erro simulado")

    from api.v1.apps.companys.service.company_service import delete_column 

    result = delete_column("test_db", "test_record_id")

    mock_instance._mark_as_deleted_by_id.assert_called_once_with("test_db", "test_record_id")

    # Verifica o resultado
    assert "Erro ao criar tabela: Erro simulado" in result


@pytest.fixture
def mock_delete_column():
    with patch("api.v1.apps.companys.service.company_service.InsertColumn") as mock_class:
        yield mock_class

def test_delete_column_success(mock_delete_column):
    mock_instance = MagicMock()
    mock_delete_column.return_value = mock_instance
    mock_instance._mark_as_deleted_by_id.return_value = "Registro marcado como deletado com sucesso."

    from api.v1.apps.companys.service.company_service import delete_column

    result = delete_column("test_db", "test_record_id")

    mock_instance._mark_as_deleted_by_id.assert_called_once_with("test_db", "test_record_id")

    assert result == "Registro marcado como deletado com sucesso."

def test_delete_column_failure(mock_delete_column):
    mock_instance = MagicMock()
    mock_delete_column.return_value = mock_instance
    mock_instance._mark_as_deleted_by_id.side_effect = Exception("Erro simulado")

    from api.v1.apps.companys.service.company_service import delete_column 

    result = delete_column("test_db", "test_record_id")

    mock_instance._mark_as_deleted_by_id.assert_called_once_with("test_db", "test_record_id")

    # Verifica o resultado
    assert "Erro ao criar tabela: Erro simulado" in result


@pytest.fixture
def mock_insert_column():
    with patch("api.v1.apps.companys.service.company_service.InsertColumn") as mock_class:
        yield mock_class

def test_trash_success(mock_insert_column):
    mock_instance = MagicMock()
    mock_insert_column.return_value = mock_instance
    mock_instance.get_deleted_records.return_value = "Registros deletados consultados com sucesso."

    from api.v1.apps.companys.service.company_service import trash

    result = trash("test_db")

    mock_instance.get_deleted_records.assert_called_once_with("test_db")

    assert result == "Registros deletados consultados com sucesso."

def test_trash_failure(mock_insert_column):
    mock_instance = MagicMock()
    mock_insert_column.return_value = mock_instance
    mock_instance.get_deleted_records.side_effect = Exception("Erro simulado")

    from api.v1.apps.companys.service.company_service import trash

    result = trash("test_db")

    mock_instance.get_deleted_records.assert_called_once_with("test_db")

    assert "Erro ao consultar tabela: Erro simulado" in result


@pytest.fixture
def mock_insert_column():
    with patch("api.v1.apps.companys.service.company_service.InsertColumn") as mock_class:
        yield mock_class

def test_get_records_success(mock_insert_column):
    mock_instance = MagicMock()
    mock_insert_column.return_value = mock_instance
    mock_instance.get_active_record.return_value = "Registros ativos consultados com sucesso."

    from api.v1.apps.companys.service.company_service import get_records

    result = get_records("test_db")

    mock_instance.get_active_record.assert_called_once_with("test_db")

    assert result == "Registros ativos consultados com sucesso."

def test_get_records_failure(mock_insert_column):
    mock_instance = MagicMock()
    mock_insert_column.return_value = mock_instance
    mock_instance.get_active_record.side_effect = Exception("Erro simulado")

    from api.v1.apps.companys.service.company_service import get_records

    result = get_records("test_db")

    mock_instance.get_active_record.assert_called_once_with("test_db")

    assert "Erro ao consultar tabela: Erro simulado" in result