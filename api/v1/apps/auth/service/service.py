from helpers.connection_api import AuthAPI
from helpers.auth_utils import JWTBearer, format_jwt

api = AuthAPI()

def get_token(email: str, password: str) -> dict:
    """ Gera o token que vem da API Auth """
    response = api.api_auth_get_token(email_auth=email, password_auth=password)
    return response
    
def create_users(name: str, email: str, username: str, password: str, image: str, token: str) -> dict:
    """ Criação de usuários por meio da API Auth """
    response = api.api_auth_create_user(name_user=name, email_user=email, user_name=username, password_user=password, image_auth=image, token_auth=token)
    return response 

def update_user(user_id: str, name: str = '', email: str = '', username: str = '', image: str = '') -> dict:
    """ Atualizar usuário """
    response = api.api_auth_update_user(user_id=user_id, name=name, email=email, username=username, image=image)
    return response

def delete_user(user_id: str, token_auth: str):
    """ Deleta os usuários """
    response = api.api_auth_delete_user(user_id=user_id, token_auth=token_auth)
    return response 

def create_role(company: int, user: int, role: str, token: str) -> dict:
    """ Criação de níveis de acesso por meio da API Auth """
    response = api.api_auth_create_role(user_id=user, company_id=company, role=role, token_auth=token)
    return response
    
def reset_password(email: str) -> dict:
    """ Envio de link ao e-mail para a senha ser alterada """
    response = api.api_auth_reset_password(email=email)
    return response
    
def reset_password_confirm(uid: str, token: str, new_password: str) -> dict:
    """ Alterar senha """
    response = api.api_auth_confirm_reset_password(uid=uid, token=token, new_password=new_password)
    return response 
    
def get_token_slug(dependencies):
    """Retornar slug """
    decoded_payload = format_jwt(dependencies)
    return decoded_payload.get('company_slug')