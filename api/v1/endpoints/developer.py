from api.v1.apps.developer.service.services import migrate_company_service
from fastapi import APIRouter, HTTPException, status, Response
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from loguru import logger
from typing import Dict, List, Any

router = APIRouter()

@router.post("/migrate-company-table/", responses={
    201: {
        "description": "Tabela criada com sucesso",
        "content": {
            "application/json": {
                "example": [
                    
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def migrate_company_table() -> str:
    """
    Faz a migração da tabela de company no banco de dados. 
    Endpoint mais específico para primeira utilização do hands-on ou testes."""
    try:
                
        migrate_company_service()

        return JSONResponse(status_code=status.HTTP_201_CREATED, content=f"Migração da tabela company realizada com sucesso.")
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")