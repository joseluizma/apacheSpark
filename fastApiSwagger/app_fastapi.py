from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBearer
from auth import create_access_token, get_current_user
from models import User, UserUpdate, Task, TaskCreate, Token
from services import UserService, TaskService

app = FastAPI(
    title="Micro-serviço com JWT",
    description="API com autenticação JWT, gerenciamento de usuários e tarefas",
    version="1.0"
)

security = HTTPBearer()

# Instanciar serviços
user_service = UserService()
task_service = TaskService()

# Endpoints de autenticação
@app.post("/register", response_model=dict)
async def register(user: User):
    try:
        await user_service.register_user(user)
        return {"message": "Usuário registrado com sucesso"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/login", response_model=Token)
async def login(user: User):
    token = await user_service.login_user(user)
    if not token:
        raise HTTPException(status_code=401, detail="Credenciais inválidas")
    return token

# Endpoints de usuários
@app.get("/users/me", response_model=dict)
async def get_current_user_info(current_user: str = Depends(get_current_user)):
    user = await user_service.get_user_by_username(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")
    return {"username": user["username"], "id": user["id"]}

@app.put("/users/me", response_model=dict)
async def update_user(user_update: UserUpdate, current_user: str = Depends(get_current_user)):
    try:
        updated_user = await user_service.update_user(current_user, user_update)
        return {"message": "Usuário atualizado com sucesso", "username": updated_user["username"]}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/users/me", response_model=dict)
async def delete_user(current_user: str = Depends(get_current_user)):
    await user_service.delete_user(current_user)
    return {"message": "Usuário deletado com sucesso"}

# Endpoints de tarefas
@app.post("/tasks", response_model=dict)
async def create_task(task: TaskCreate, current_user: str = Depends(get_current_user)):
    task_id = await task_service.create_task(task, current_user)
    return {"message": "Tarefa criada com sucesso", "task_id": task_id}

@app.get("/tasks", response_model=list[Task])
async def get_tasks(current_user: str = Depends(get_current_user)):
    tasks = await task_service.get_user_tasks(current_user)
    return tasks

@app.get("/tasks/{task_id}", response_model=Task)
async def get_task(task_id: int, current_user: str = Depends(get_current_user)):
    task = await task_service.get_task(task_id, current_user)
    if not task:
        raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    return task

@app.put("/tasks/{task_id}", response_model=dict)
async def update_task(task_id: int, task: TaskCreate, current_user: str = Depends(get_current_user)):
    try:
        await task_service.update_task(task_id, task, current_user)
        return {"message": "Tarefa atualizada com sucesso"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tasks/{task_id}", response_model=dict)
async def delete_task(task_id: int, current_user: str = Depends(get_current_user)):
    try:
        await task_service.delete_task(task_id, current_user)
        return {"message": "Tarefa deletada com sucesso"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)