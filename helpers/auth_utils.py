from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Request
from fastapi.exceptions import HTTPException
import jwt
import base64
from jwt.exceptions import InvalidTokenError
import json
from decouple import config


ALGORITHM = "HS256"
JWT_SECRET_KEY = 'django-insecure-rdze9$g(08c#!zimo8ix%to7wa6ki&to=-_1wk$0#+i-u29!'


def decodeJWT(jwtoken: str):
    try:
        payload = jwt.decode(jwt=jwtoken,
                              key=JWT_SECRET_KEY,
                              algorithms=[ALGORITHM])
        return payload
    except InvalidTokenError:
        return None

class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(JWTBearer, self).__call__(request)
        if credentials:
            if not credentials.scheme == 'Bearer':
                raise HTTPException(status_code=403, detail="Invalid authentication scheme.")
            if not self.verify_jwt(credentials.credentials):
                raise HTTPException(status_code=403, detail="Invalid token or expired token.")

            return credentials.credentials
        else: 
            raise HTTPException(status_code=403, detail="Invalid authorization code.")
        
        
    def verify_jwt(self, jwtoken: str) -> bool:
        isTokenValid: bool = False

        try:
            payload = decodeJWT(jwtoken)
        except:
            payload = None
        if payload:
            isTokenValid = True
        return isTokenValid
    

jwt_bearer = JWTBearer()


def format_jwt(token):
    """ Transforma JWT que está em base64 para string e coloca dentro de uma lista. Com isso é possível pegar um dado específico do token """
    token_decode = token.split(".")[1]

    padding = '=' * (4 - len(token_decode) % 4)
    token_decode += padding

    decoded_bytes = base64.urlsafe_b64decode(token_decode)
    decoded_str = decoded_bytes.decode('utf-8')

    decoded_payload = json.loads(decoded_str)

    return decoded_payload