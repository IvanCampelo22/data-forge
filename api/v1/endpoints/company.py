from api.v1.apps.companys.service.company_service import create_company_service, get_company_service, trash_company_service, deactive_company_service, active_company_service, update_company_service
from fastapi import APIRouter, HTTPException, status, Response, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from decouple import config
from typing import Dict, Optional
from helpers.utils import normalize_string
import re

router = APIRouter()


@router.get(
    "/get-company/",
    responses={
        200: {
            "description": "Lista de empresas ativas consultada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 100,
                        "page_size": 10,
                        "current_offset": 0,
                        "companies": [
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
def all_company(
    limit: Optional[int] = Query(10, description="Número máximo de empresas por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """
    Lista todas as empresas ativas de forma paginada.
    """
    try:
        # Chama o serviço com paginação
        company_objects = get_company_service(limit, offset)

        if not company_objects or not company_objects.get("companies"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhuma empresa ativa encontrada.")

        return jsonable_encoder(company_objects)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get(
    "/trash-company/",
    responses={
        200: {
            "description": "Lixeira consultada com sucesso",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 50,
                        "page_size": 10,
                        "current_offset": 0,
                        "inactive_companies": [
                            {
                                "id": 1,
                                "name": "Empresa Inativa X",
                                "deactivated_at": "2024-01-01T10:30:00"
                            }
                        ]
                    }
                }
            },
        },
        400: {"description": "Insira dados válidos"},
        404: {"description": "Nenhuma empresa inativa encontrada"},
        500: {"description": "Erro interno"},
    },
    status_code=status.HTTP_200_OK
)
def trash_company(
    limit: Optional[int] = Query(10, description="Número máximo de empresas por página", ge=1),
    offset: Optional[int] = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0),
):
    """
    Listagem paginada de todas as empresas desativadas.
    """
    try:
        # Chama o serviço com paginação
        trash_company_objects = trash_company_service(limit, offset)

        if not trash_company_objects or not trash_company_objects.get("inactive_companies"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhuma empresa inativa encontrada.")

        return jsonable_encoder(trash_company_objects)

    except HTTPException as exception:
        raise exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/insert-company/", responses={
    201: {
        "description": "Empresa criada com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {
                        "company": [
                        {
                            "name": "braskem",
                            "cnpj": "123456789000",
                            "email_company": "company@gmail.com"
                        }
                        ]
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def create_company(json: dict) -> Dict[str, str]:
    """Insere empresas no banco de dados"""
    try:
        
        if not isinstance(json.get('company', []), list):
            raise HTTPException(status_code=400, detail="'tables' deve ser uma lista de objetos")

        for data in json['company']:
            name = data.get('name')
            cnpj = data.get('cnpj')
            email_company = data.get('email_company')

            if not name: 
                raise HTTPException(status_code=400, detail="Insira o nome da empresa")
            
            if not cnpj:
                raise HTTPException(status_code=400, detail="Insira o CNPJ da empresa")
            
            if len(cnpj) != 14:
                raise HTTPException(status_code=400, detail="O CNPJ deve conter 14 dígitos. Por favor, insira apenas valores numéricos")

            valid_email = re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_company)

            if not valid_email:
                raise HTTPException(status_code=400, detail=f"O {email_company} é inválido. Insira o valor correto para continuar")

            create = create_company_service(name_company=name, cnpj_company=cnpj, email_company=email_company)

        return JSONResponse(status_code=200, content="Empresa criada com sucesso")
    
    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.put(
    "/active-company/",
    responses={
        201: {
            "description": "Empresa removida da lixeira com sucessp.",
            "content": {
                "application/json": {
                    "company_id": 1
                }
            },
        },
        400: {"description": "Insira dados válidos"}
    },
    status_code=status.HTTP_201_CREATED
)
def active_company(company_id: str = None) -> Dict[str, str]:
    """Remove empresas da lixeira"""
    try:
        if not company_id:
            raise HTTPException(status_code=404, detail="Não há empresas com esse identifcador")
        
        active_company_objects = active_company_service(company_id)
         
        if not active_company_objects:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empresa não encontrada")

        return JSONResponse(status_code=200, content="Empresa reativada com sucesso")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.put(
    "/update-company/",
    responses={
        201: {
            "description": "Empresa atualizada com sucesso",
            "content": {
            "application/json": {
                "example": [
                    {
                        "company": [
                        {   
                            "company_id": 1,
                            "name": "braskem",
                            "cnpj": "123456789000",
                            "email_company": "company@gmail.com"
                        }
                        ]
                    }
                ]
            }
        },
        },
        400: {"description": "Insira dados válidos"}
    },
    status_code=status.HTTP_201_CREATED
)
def update_company(json: dict) -> Dict[str, str]:
    """
    Atualiza dados da empresa.
    """
    try:
        for data in json['company']:
            company_id = data.get("company_id")
            name = data.get('name')
            cnpj = data.get('cnpj')
            email_company = data.get('email_company')
            
            if not company_id:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insira um identificador válido para a empresa.")

            if len(cnpj) != 14:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="O CNPJ deve conter 14 dígitos. Por favor, insira apenas valores numéricos")

            valid_email = re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_company)

            if not valid_email:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"O {email_company} é inválido. Insira o valor correto para continuar")
            
            update_company_objects = update_company_service(company_id, name_company=name, cnpj_company=cnpj, email_company=email_company)
            
            if not update_company_objects:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Empresa não encontrada")

        return JSONResponse(status_code=status.HTTP_200_OK, content="Empresa atualizada com sucesso.")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.delete(
    "/delete-company/",
    responses={
        201: {
            "description": "Lixeira consultada com sucesso",
            "content": {
                "application/json": {
                    "company_id": 1
                }
            },
        },
        400: {"description": "Insira dados válidos"}
    },
    status_code=status.HTTP_201_CREATED
)
def delete_company(company_id: str) -> Dict[str, str]:
    """Desativa empresa por meio do seu identificador no banco de dados"""
    try:

        if not company_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"É necessário passar o identificador da empresa.")

        delete_company_objects = deactive_company_service(company_id)

        if not delete_company_objects:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Nenhuma empresa encontrada.")

        return JSONResponse(status_code=200, content="Empresa deletada com sucesso")

    except HTTPException as exception:
        raise exception
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")