import os 
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List

load_dotenv()

#Clipping secrets
DB_NAME_CLIPPING=os.environ.get("DB_NAME_CLIPPING")
DB_USER_CLIPPING=os.environ.get("DB_USER_CLIPPING")
DB_PASSWORD_CLIPPING=os.environ.get("DB_PASSWORD_CLIPPING")
DB_HOST_CLIPPING=os.environ.get("DB_HOST_CLIPPING")
DB_OPTIONS_CLIPPING=os.environ.get("DB_OPTIONS_CLIPPING")

#Handson secrets
DB_NAME_HANDSON=os.environ.get("DB_NAME_HANDSON")
DB_USER_HANDSON=os.environ.get("DB_USER_HANDSON")
DB_PASSWORD_HANDSON=os.environ.get("DB_PASSWORD_HANDSON")
DB_HOST_HANDSON=os.environ.get("DB_HOST_HANDSON")

SUPER_ADMIN = os.environ.get('SUPER_ADMIN')
ADMIN = os.environ.get('ADMIN')
VIEWER = os.environ.get('VIEWER')
