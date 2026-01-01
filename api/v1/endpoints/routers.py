from fastapi.routing import APIRouter
from api.v1.endpoints import company
from api.v1.endpoints import auth
from api.v1.endpoints import files
from api.v1.endpoints import clipping
from api.v1.endpoints import custom
from api.v1.endpoints import manager
from api.v1.endpoints import developer

api_router = APIRouter()

api_router.include_router(custom.router, prefix='/custom', tags=['custom'])
api_router.include_router(company.router, prefix='/company', tags=['company'])
api_router.include_router(auth.router, prefix='/auth', tags=['auth'])
api_router.include_router(files.router, prefix='/file', tags=['file'])
api_router.include_router(clipping.router, prefix="/clipping", tags=["clipping"])
api_router.include_router(manager.router, prefix="/manager", tags=["manager"])
api_router.include_router(developer.router, prefix="/developer", tags=["developer"])