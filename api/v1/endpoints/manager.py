from api.v1.apps.manager.service.manager_service import delete_table_service, delete_schema_service, delete_column_service
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from helpers.utils import normalize_string

router = APIRouter()


@router.delete("/delete-table/", responses={
    201: {
        "description": "Tabela deletada com sucesso",
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
def delete_table(json: dict) -> str:
    """Deleta um tabela em um schema específico no banco de dados"""
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
                
                delete_table_service(table_name, schema_name)

        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Tabela {table_name} criada com sucesso.")
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    

@router.delete("/delete-schema/", responses={
    201: {
        "description": "Schema deletado com sucesso",
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
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def delete_schema(json: dict) -> str:
    """Deleta um schema em um schema específico no banco de dados"""
    try:
        
        if not isinstance(json.get('schemas', []), list):
            raise HTTPException(status_code=400, detail="'schemas' deve ser uma lista de objetos")

        for data in json['schemas']:
            schema_name = data.get('schema_name')
            schema_name = normalize_string(schema_name)

            if not schema_name:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para o nome do schema")
                
            delete_schema_service(schema_name)

        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Schema {schema_name} deletado com sucesso.")
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
    
@router.delete("/delete-column/", responses={
    201: {
        "description": "Coluna deletada com sucesso",
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
                        ],
                        "columns": [
                            {
                            "column_name": "value"
                            },
                            {
                            "column_name": "reach"
                            }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def delete_column(json: dict) -> str:
    """Deleta uma coluna de uma tabela específica no banco de dados"""
    try:
        
        if not isinstance(json.get('tables', []), list):
            raise HTTPException(status_code=400, detail="'tables' deve ser uma lista de objetos")

        for data in json['tables']:
            table_name = data.get('table_name')
            table_name = table_name.lower()

            if not table_name:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para o nome da tabela")

            for data in json['schemas']:
                schema_name = data.get('schema_name')
                schema_name = schema_name.lower()
                
                if not schema_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para o schema")
                
                for data in json['columns']:
                    column_name = data.get('column_name')
                    column_name = column_name.lower()

                    if not column_name:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um valor válido para a coluna")
                
                    delete_column_service(table_name, schema_name, column_name)

        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Coluna {column_name} deletada com sucesso.")
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")