from pydantic import BaseModel
from typing import Optional

class User(BaseModel):
    username: str
    password: str

class UserUpdate(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None

class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None

class Task(BaseModel):
    id: int
    title: str
    description: Optional[str] = None
    user_id: int

class Token(BaseModel):
    access_token: str
    token_type: str