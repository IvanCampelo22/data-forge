from api.v1.apps.clipping.service.clipping_service import  get_active_company_clipping, get_deactivate_company_clipping, transfer_active_company_to_handson
from fastapi import APIRouter, HTTPException, status, Query
from fastapi.encoders import jsonable_encoder
from typing import Optional

router = APIRouter()

@router.get(
    "/get-active-company-clipping/",
    responses={
        200: {
            "description": "Empresas recuperadas com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 50,
                        "page_size": 10,
                        "current_offset": 0,
                        "active_companies": [
                            {
                                "id": 1,
                                "name": "Empresa X",
                                "created_at": "2024-01-01T10:30:00"
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Nenhuma empresa ativa encontrada"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def active_company(
    limit: Optional[int] = Query(10, description="Número máximo de empresas por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """
    Lista todas as empresas ativas no clipping de forma paginada.
    """
    try:
        objs = get_active_company_clipping(limit, offset)

        if not objs or not objs.get("active_companies"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhuma empresa ativa encontrada.")

        return jsonable_encoder(objs)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    

@router.get(
    "/get-deactive-company-clipping/",
    responses={
        200: {
            "description": "Empresas desativadas recuperadas com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 50,
                        "page_size": 10,
                        "current_offset": 0,
                        "deactivated_companies": [
                            {
                                "id": 1,
                                "name": "Empresa Y",
                                "deactivated_at": "2024-01-01T10:30:00"
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Nenhuma empresa desativada encontrada"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def deactive_company(
    limit: Optional[int] = Query(10, description="Número máximo de empresas por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """
    Lista todas as empresas desativadas no clipping de forma paginada.
    """
    try:
        objs = get_deactivate_company_clipping(limit, offset)

        if not objs or not objs.get("deactivated_companies"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhuma empresa desativada encontrada.")

        return jsonable_encoder(objs)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
    
@router.post(
    "/transfer-active-company/",
    responses={
        201: {
            "description": "Empresa transferida com sucesso",
            "content": {"application/json": {}},
        },
        400: {"description": "Nome da empresa inválido"},
        404: {"description": "Empresa não encontrada ou inativa"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_201_CREATED
)
def transfer_active_company(company_name: str):
    """
    Endpoint para buscar uma empresa ativa pelo nome e transferi-la para `handson.company`.
    """
    try:
        if not company_name:
            raise HTTPException(
                status_code=400, detail="Nome da empresa é obrigatório"
            )
        result = transfer_active_company_to_handson(company_name)
        return jsonable_encoder({"message": result})

    except HTTPException as he:
        raise he

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro interno: {str(e)}"
        )