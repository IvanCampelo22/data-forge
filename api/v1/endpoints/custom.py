from api.v1.apps.companys.service.customize_company import create_table_service, create_column_text_service, create_column_integer_service, create_column_float_service, create_column_date_service, create_column_boolean_service, update_field_name_service, change_type_field_service, delete_column_service, active_register_service, create_schema_service
from api.v1.apps.companys.service.company_service import trash_register_service, get_records_service, add_table_in_company_service
from api.v1.apps.companys.service.filters import filter_date_service, filter_company_service, filter_trash_service
from fastapi import APIRouter, HTTPException, status, Response, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from loguru import logger
from typing import Dict, List, Any, Optional
from helpers.utils import normalize_string

router = APIRouter()

# TODO verificar se o tipo do campo padrão vai ser str ou date, para alterar o tipo dentro do isinstance e validar corretamente
@router.get(
    "/filter-by-date/",
    responses={
        200: {
            "description": "Listagem realizada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 50,
                        "page_size": 10,
                        "current_offset": 0,
                        "data": [
                            {
                                "id": 1,
                                "title": "Notícia 1",
                                "date": "2024-01-01",
                                "content": "Exemplo de conteúdo",
                            }
                        ],
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def filter_by_date(
    table_name: str = Query(..., description="Nome da tabela"),
    schema_name: str = Query(..., description="Nome do schema"),
    start_date: str = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    end_date: str = Query(..., description="Data final no formato YYYY-MM-DD"),
    limit: Optional[int] = Query(10, description="Número máximo de registros por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """Listagem dos registros por filtro em range de data com paginação"""

    try:
        # Validação adicional do formato das datas
        if not start_date or not end_date:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma data válida no formato YYYY-MM-DD.")

        if not isinstance(start_date, str) or not isinstance(end_date, str):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Datas devem ser strings no formato YYYY-MM-DD.")

        table_name = normalize_string(table_name)
        schema_name = normalize_string(schema_name)
        filter_by_date_objects = filter_date_service(table_name, schema_name, start_date, end_date, limit, offset)

        if not filter_by_date_objects or not filter_by_date_objects.get("data"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhuma informação encontrada para o período informado.")

        return jsonable_encoder(filter_by_date_objects)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        logger.error(f"Erro no endpoint filter-by-date: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro interno: {str(e)}")


# TODO verificar se o tipo do campo padrão vai ser str ou date, para alterar o tipo dentro do isinstance e validar corretamente
@router.get(
    "/filter-trash-by-date/",
    responses={
        200: {
            "description": "Listagem realizada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 50,
                        "page_size": 10,
                        "current_offset": 0,
                        "deleted_records": [
                            {
                                "id": 1,
                                "name": "Registro deletado",
                                "deleted_at": "2024-01-01T10:30:00"
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Nenhum registro deletado encontrado"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def filter_trash_by_date(
    table_name: str = Query(..., description="Nome da tabela"),
    schema_name: str = Query(..., description="Nome do schema"),
    start_date: str = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    end_date: str = Query(..., description="Data final no formato YYYY-MM-DD"),
    limit: Optional[int] = Query(10, description="Número máximo de registros por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """
    Listagem paginada de registros deletados dentro da lixeira por intervalo de datas.
    """
    try:
        if not table_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Valor inválido para o nome da tabela.")

        if not start_date or not end_date:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma data válida no formato YYYY-MM-DD.")

        if not isinstance(start_date, str) or not isinstance(end_date, str):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Datas devem ser strings no formato YYYY-MM-DD.")

        table_name = normalize_string(table_name)
        schema_name = normalize_string(schema_name)

        filter_trash_objects = filter_trash_service(table_name, schema_name, start_date, end_date, limit, offset)

        if not filter_trash_objects or not filter_trash_objects.get("deleted_records"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum registro deletado encontrado nesse intervalo de datas.")

        return jsonable_encoder(filter_trash_objects)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        logger.error(f"Erro no endpoint filter-trash-by-date: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.get(
    "/filter-by-company/",
    responses={
        200: {
            "description": "Listagem realizada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_tables": 5,
                        "page_size": 10,
                        "current_offset": 0,
                        "tables": [
                            {
                                "table_name": "company_data",
                                "columns": [
                                    {"name": "id", "type": "integer"},
                                    {"name": "name", "type": "varchar"},
                                    {"name": "created_at", "type": "timestamp"}
                                ]
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Nenhuma informação encontrada"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def filter_by_company(
    company_id: str = Query(..., description="Identificador da empresa"),
    schema_name: str = Query(..., description="Nome do schema no banco de dados"),
    limit: Optional[int] = Query(10, description="Número máximo de tabelas por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de tabelas a serem ignoradas antes de retornar os resultados", ge=0),
):
    """
    Lista de registros pelo identificador da empresa com suporte a paginação.
    """
    try:
        if not company_id: 
            raise HTTPException(status_code=400, detail="Insira o identificador da empresa.")

        # Chama o serviço com paginação
        schema_name = normalize_string(schema_name)
        filter_company_objects = filter_company_service(company_id, schema_name, limit, offset)

        if not filter_company_objects or not filter_company_objects.get("tables"):
            raise HTTPException(status_code=404, detail="Nenhuma informação encontrada para o identificador informado.")

        return jsonable_encoder(filter_company_objects)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        logger.error(f"Erro no endpoint filter-by-company: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get(
    "/trash-registers/",
    responses={
        200: {
            "description": "Consulta realizada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 5,
                        "page_size": 10,
                        "current_offset": 0,
                        "trash": [
                            {
                                "table_name": "braskem",
                                "deleted_at": "2024-01-01T10:30:00",
                                "record_data": {"id": 1, "name": "Registro deletado"}
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Não há registros na lixeira"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def get_trash(
    table_name: str = Query(..., description="Nome da tabela"),
    schema_name: str = Query(..., description="Nome do schema"),
    limit: Optional[int] = Query(10, description="Número máximo de registros por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
) -> Dict[str, Any]:
    """Lixeira de registros deletados com suporte a paginação."""
    
    try:
        if not table_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um nome válido para a tabela.")
        
        table_name = normalize_string(table_name)
        schema_name = normalize_string(schema_name)
        trash_register_objects = trash_register_service(table_name, schema_name, limit, offset)

        if not trash_register_objects or not trash_register_objects.get("trash"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Não há nada na lixeira.")

        return jsonable_encoder(trash_register_objects)
    
    except HTTPException as exception:
        raise exception

    except Exception as e:
        logger.error(f"Erro no endpoint trash-registers: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get(
    "/get_records/",
    responses={
        200: {
            "description": "Consulta realizada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 100,
                        "page_size": 10,
                        "current_offset": 0,
                        "active_records": [
                            {
                                "id": 1,
                                "name": "Registro ativo 1",
                                "created_at": "2024-01-01T10:30:00"
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Não foi possível encontrar dados"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def get_registers(
    table_name: str = Query(..., description="Nome da tabela"),
    schema_name: str = Query(..., description="Nome do schema"),
    limit: Optional[int] = Query(10, description="Número máximo de registros por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """Traz todos os registros ativos em uma tabela consultada com suporte a paginação."""
    try:
        if not table_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para o nome da tabela.")

        table_name = normalize_string(table_name)
        schema_name = normalize_string(schema_name)
        get_records_objects = get_records_service(table_name, schema_name, limit, offset)

        if not get_records_objects or not get_records_objects.get("active_records"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Não foi possível encontrar dados.")

        return jsonable_encoder(get_records_objects)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        logger.error(f"Erro no endpoint get_records: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/create-table/", responses={
    201: {
        "description": "Tabela criada com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def add_table(json: dict) -> str:
    """Cria a tabela em um schema específico no banco de dados"""
    try:
        
        if not isinstance(json.get('tables', []), list):
            raise HTTPException(status_code=400, detail="'tables' deve ser uma lista de objetos")

        for data in json['tables']:
            table_name = data.get('table_name')
            table_name = normalize_string(table_name)

            if not table_name:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para o nome da tabela")

            for data in json['schemas']:
                schema_name = data.get('schema_name')
                schema_name = normalize_string(schema_name)
                
                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para o schema")
                table_name = normalize_string(table_name)
                schema_name = normalize_string(schema_name)
                create = create_table_service(table_name, schema_name)

        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Tabela {table_name} criada com sucesso.")
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.post("/create-schema/", responses={
    201: {
        "description": "Schema criado com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"},
    500: {"description": "Erro interno no servidor"}
}, status_code=status.HTTP_201_CREATED)
def add_schema(json: dict) -> JSONResponse:
    """Cria um schema no banco de dados"""
    try:
        if not isinstance(json.get('schemas', []), list):
            raise HTTPException(status_code=400, detail="'schemas' deve ser uma lista de objetos")

        for data in json['schemas']:
            schema_name = data.get('schema_name')

            if not schema_name:
                raise HTTPException(status_code=400, detail="Cada item em 'schemas' deve conter 'schema_name'")

            schema_name = normalize_string(schema_name)
            create = create_schema_service(schema_name)
            logger.info(f"Schema {schema_name} criado com sucesso.")

        return JSONResponse(status_code=status.HTTP_201_CREATED, content={"message": f"Schema {schema_name} criado com sucesso."})
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        logger.error(f"Erro interno ao criar schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/create-column-text/", responses={
    201: {
        "description": "Colunas criadas com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem"
                        }
                        ],
                        "columns": [
                        {
                            "column_name": "DATA"
                        },
                        {
                            "column_name": "MÊS"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def add_column_text(payload: dict) -> Dict[str, str]:
    """Criar campo do tipo texto para uma tabela específica"""
    try:
        tables = payload.get("tables", [])
        schemas = payload.get("schemas", [])
        columns = payload.get("columns", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=400, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")

            if not table_name:
                raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da tabela.")
            
            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")
                
                for column in columns:
                    column_name = column.get("column_name")

                    if not column_name:
                        raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da coluna.")
                    
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    column_name = normalize_string(column_name)
                    create = create_column_text_service(table_name, schema_name, column_name)
            
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Coluna de texto criada com sucesso")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    

@router.post("/create-column-number/", responses={
    201: {
        "description": "Colunas criadas com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem"
                        }
                        ],
                        "columns": [
                        {
                            "column_name": "DATA"
                        },
                        {
                            "column_name": "MÊS"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def add_column_integer_number(payload: dict) -> Dict[str, str]:
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=400, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da tabela")

            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")

                for column in columns:
                    column_name = column.get("column_name")
                    if not column_name:
                        raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da coluna")
                    
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    column_name = normalize_string(column_name)

                    create = create_column_integer_service(table_name, schema_name, column_name)
            
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Coluna de número inteiro criada com sucesso")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.post("/create-column-float/", responses={
    201: {
        "description": "Colunas criadas com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem"
                        }
                        ],
                        "columns": [
                        {
                            "column_name": "value"
                        },
                        {
                            "column_name": "media_valuation"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def add_column_float_number(payload: dict) -> Dict[str, str]:
    """Adiciona uma coluna do tipo float"""
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=400, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da tabela.")


            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")

                for column in columns:
                    column_name = column.get("column_name")
                    if not column_name:
                        raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da coluna.")
                    
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    column_name = normalize_string(column_name)
                    create = create_column_float_service(table_name, schema_name, column_name)
            
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Coluna de número float criada com sucesso")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/create-column-date/", responses={
    201: {
        "description": "Colunas criadas com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem"
                        }
                        ],
                        "columns": [
                        {
                            "column_name": "DATA"
                        },
                        {
                            "column_name": "MÊS"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def add_column_date(payload: dict) -> Dict[str, str]:
    """Adiciona uma coluna do tipo date"""
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=400, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da tabela")

            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")

                for column in columns:
                    column_name = column.get("column_name")
                    if not column_name:
                        raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da coluna")
                
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    column_name = normalize_string(column_name)
                    create = create_column_date_service(table_name, schema_name, column_name)
            
            return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Coluna de date criada com sucesso")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.post("/create-column-boolean/", responses={
    201: {
        "description": "Colunas criadas com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem"
                        }
                        ],
                        "columns": [
                        {
                            "column_name": "is_active"
                        },
                        {
                            "column_name": "is_client"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def add_column_boolean(payload: dict) -> Dict[str, str]:
    """Adiciona uma coluna do tipo boolean"""
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=400, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da tabela")

            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")

                for column in columns:
                    column_name = column.get("column_name")
                    if not column_name:
                        raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da coluna")
                    
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    column_name = normalize_string(column_name)
                    create = create_column_boolean_service(table_name, schema_name, column_name)

        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Coluna de boolean  criada com sucesso")
            
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.post("/associate-company-with-project/", responses={
    201: {
        "description": "Associa empresa a um projeto",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                        {
                            "table_name": "braskem",
                            "company_id": "1"
                        }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def associate_company_with_project(table_name: str = None, schema_name: str = None, company_id: str = None):
    """
    Associa uma empresa existente a um projeto
    Para isso, é necessário primeiro adicionar outros registros no projeto
    Para não criar registros vazios apenas com o company_id
    """
    try:

        if not table_name:
            raise HTTPException(status_code=400, detail="Insira um valor válido para o nome da tabela.")
        
        if not company_id:
            raise HTTPException(status_code=400, detail="Não foi possível encontrar nenhuma empresa com esse identificador.")
        
        table_name = normalize_string(table_name)
        schema_name = normalize_string(schema_name)
        add_table_in_company_service(table_name, schema_name, company_id)
        
        return JSONResponse(status_code=status.HTTP_200_OK, content=f"Empresa associada ao projeto {table_name} com sucesso")
            
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.put("/rename-field/", responses={
    201: {
        "description": "Dados deletados com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                            {
                                "table_name": "coca"
                            }
                        ],
                        "columns": [
                            {
                                "old_name": "name",
                                "new_name": "values"

                            }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def rename_field(payload: dict):
    """Atualiza o nome de um campo ou mais, em uma tabela específica"""
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status_code=400, detail="Cada item em 'tables' deve conter 'table_name'")

            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")

                for column in columns:
                    old_name = column.get("old_name")
                    new_name = column.get("new_name")

                    if not old_name or not new_name:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Antigo nome e o novo nome, são campos obrigatórios.")

                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    column_name = normalize_string(column_name)
                    update_field_name_service(table_name=table_name, schema_name=schema_name, old_field_name=old_name, new_field_name=new_name)
            
            return JSONResponse(status_code=status.HTTP_200_OK, content="Tabelas renomeadas com sucesso.")
            
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.put("/update-type-field/", responses={
    201: {
        "description": "Dados atualizados com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                            {
                                "table_name": "coca"
                            }
                        ],
                        "fields": [
                            {
                                "field_name": "name",
                            }
                        ],
                        "fields_type": [
                            {
                                "field_type": "varchar"
                            }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def update_type_field(payload: dict):
    """Altera o tipo do campo de uma tabela específica"""
    try:
        tables = payload.get("tables", [])
        fields = payload.get("fields", [])
        fields_type = payload.get("fields_type", [])
        schemas = payload.get("schemas", [])

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status_code=400, detail="Cada item em 'tables' deve conter 'table_name'")
            
            table_name = table_name.lower()

            for field in fields:
                field_name = field.get("field_name")
                if not field_name:
                    raise HTTPException(status_code=400, detail="É necessário informar o nome do campo")
                
                for schema in schemas:
                    schema_name = schema.get("schema_name")

                    if not schema_name:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")
                    
                    for field_type_item in fields_type:
                        field_type = field_type_item.get("field_type")
                        if not field_type:
                            raise HTTPException(status_code=400, detail="É necessário informar o tipo do campo")
                        
                        table_name = normalize_string(table_name)
                        schema_name = normalize_string(schema_name)
                        change_type_field_service(table_name=table_name, schema_name=schema_name, field_name=field_name, field_type=field_type)
                
                return JSONResponse(status_code=status.HTTP_200_OK, content="Tipos de campos atualizados com sucesso.")
    
    except Exception as e:
        logger.error(f"Erro ao atualizar tipo do campo: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@router.put(
    "/active-register/",
    responses={
    201: {
        "description": "Dados ativados com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                            {
                                "table_name": "braskem"
                            }
                        ],
                        "columns": [
                            {
                                "record_id": 1
                            },
                            {
                                "record_id": 2
                            }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED
)
def active_register(payload: dict) -> Dict[str, str]:
    """Restaura os registros da tabela"""
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Valor inválido para o nome da tabela")

            for schema in schemas:
                schema_name = schema.get("schema_name")

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")

                for column in columns:
                    record_id = column.get("record_id")
                    if not record_id:
                        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Insira um valor certo para o identificador do registro que deseja excluir.")
                    
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    active_register_service(table_name, schema_name, record_id)
                
        return JSONResponse(status_code=status.HTTP_200_OK, content="Coluna restaurada com sucesso.")

    except HTTPException as http_err:
        raise http_err
    
    except Exception as e:
        return {"error": str(e)}
    

@router.delete("/delete_record/", responses={
    201: {
        "description": "Dados deletados com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "tables": [
                            {
                                "table_name": "braskem"
                            }
                        ],
                        "columns": [
                            {
                                "record_id": 1
                            },
                            {
                                "record_id": 2
                            }
                        ],
                        "schemas": [
                        {
                            "schema_name": "meu_schema"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def deactive_record(payload: dict) -> Dict[str, str]:
    """Deleta um registro utilizando o softdelete"""
    try:
        tables = payload.get("tables", [])
        columns = payload.get("columns", [])
        schemas = payload.get("schemas", [])

        if not isinstance(tables, list) or not isinstance(columns, list):
            raise HTTPException(status_code=400, detail="'tables' e 'columns' devem ser listas de objetos")

        for table in tables:
            table_name = table.get("table_name")
            table_name = table_name.lower()
            if not table_name:
                raise HTTPException(status_code=400, detail="Valor inválido para o nome da tabela")

            for schema in schemas:
                schema_name = schema.get("schema_name")
                schema_name.lower()

                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira uma valor válido para o schema")
            
                for column in columns:
                    record_id = column.get("record_id")
                    if not record_id:
                        raise HTTPException(status_code=400, detail="Insira um valor certo para o identificador do registro que deseja excluir.")
                    
                    table_name = normalize_string(table_name)
                    schema_name = normalize_string(schema_name)
                    delete_column_service(table_name, schema_name, record_id)
                
        return JSONResponse(status_code=status.HTTP_200_OK, content="Coluna deletada com sucesso.")

    except HTTPException as http_err:
        raise http_err
    
    except Exception as e:
        return {"error": str(e)}