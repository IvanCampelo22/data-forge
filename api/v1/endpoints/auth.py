
from api.v1.apps.auth.service import service
from fastapi import APIRouter, status, Depends, HTTPException
from helpers.auth_utils import JWTBearer
from typing import Optional

router = APIRouter()
services = service


@router.post('/get-token', responses={
    200: {
        "description": "Token gerado com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {   
                        "email": "email@example.com",
                        "password": "12345678"
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_200_OK)
def get_token_router(email: str, password: str):
    try: 
        if email and password: 
            token =  services.get_token(email, password)
            return token
    
    except Exception as e:
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}")
    

@router.post('/create-user', responses={
    201: {
        "description": "Usuário criado com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {   
                        "name": "name example",
                        "email": "email@example.com",
                        "username": "Username12",
                        "password": "12345678",
                        "image": "image base64"
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def create_users(name: str, email: str, username: str, password: str, image:str, dependencies=Depends(JWTBearer())):
    try: 

        user = services.create_users(name, email, username, password, image, token=dependencies)
        return user
    
    except Exception as e:
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}")
    

@router.put('/update-users/{user_id}/', responses={
    200: {
        "description": "Usuário atualizado com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {   
                        "name": "name example",
                        "email": "email@example.com",
                        "username": "Username12",
                        "image": "image base64"
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
})
def update_users(user_id: str, name: Optional[str] = '', email: Optional[str] = '', username: Optional[str] = '', image: Optional[str] = '', dependencies=Depends(JWTBearer())):
    try:

        user = services.update_user(user_id, name, email, username, image)
        return user 
    
    except Exception as e:
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}")
    

@router.delete('/delete-users/{user_id}/')
def delete_users(user_id: str, dependencies=Depends(JWTBearer())):
    try:
        user = services.delete_user(user_id, token_auth=dependencies)
        return user

    except Exception as e:
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f'{e}')    

@router.post('/create-role', responses={
    201: {
        "description": "Nível de acesso criado com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {   
                        "company_id": 1,
                        "user_id": 1,
                        "role": "seuniveldeacesso"
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_201_CREATED)
def create_role(company_id: int, user_id: int, role: str, dependencies=Depends(JWTBearer())):
    try:

        role = services.create_role(company_id, user_id, role, token=dependencies)
        return role
    
    except Exception as e: 
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}")
    

@router.post('/reset-password', responses={
    201: {
        "description": "Email enviado",
        "content": {
            "application/json": {
                "example": [
                    {   
                        "email"
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_200_OK)
def reset_password(email):
    try: 

        resetpassword = services.reset_password(email)
        return resetpassword
    
    except Exception as e:
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}")
    
@router.post('/reset-password-confirm', responses={
    201: {
        "description": "Email resetado com sucesso",
        "content": {
            "application/json": {
                "example": [
                    {   
                        "uid": "123",
                        "token": "123",
                        "new_password": "12345678"
                    }
                ]
            }
        },
    },
    400: {"description": "Insira dados válidos"}
}, status_code=status.HTTP_200_OK)
def reset_password(uid, token, new_password):
    try:

        newpassword = services.reset_password_confirm(uid, token, new_password)
        return newpassword
    
    except Exception as e:
        return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}")
