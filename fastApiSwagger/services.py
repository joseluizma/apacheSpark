import sqlite3
from passlib.context import CryptContext
from models import User, UserUpdate, TaskCreate
from auth import create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from datetime import timedelta

class UserService:
    def __init__(self):
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      username TEXT UNIQUE NOT NULL, 
                      password TEXT NOT NULL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS tasks 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      title TEXT NOT NULL, 
                      description TEXT, 
                      user_id INTEGER NOT NULL, 
                      FOREIGN KEY (user_id) REFERENCES users(id))''')
        conn.commit()
        conn.close()

    async def register_user(self, user: User):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        hashed_password = self.pwd_context.hash(user.password)
        try:
            c.execute("INSERT INTO users (username, password) VALUES (?, ?)", 
                     (user.username, hashed_password))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            raise ValueError("Username já existe")
        finally:
            conn.close()

    async def login_user(self, user: User):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE username = ?", (user.username,))
        db_user = c.fetchone()
        conn.close()

        if db_user and self.pwd_context.verify(user.password, db_user[2]):
            access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            access_token = create_access_token(
                data={"sub": user.username}, expires_delta=access_token_expires
            )
            return {"access_token": access_token, "token_type": "bearer"}
        return None

    async def get_user_by_username(self, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT id, username FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        conn.close()
        if user:
            return {"id": user[0], "username": user[1]}
        return None

    async def update_user(self, username: str, user_update: UserUpdate):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        updates = []
        params = []
        if user_update.username:
            updates.append("username = ?")
            params.append(user_update.username)
        if user_update.password:
            updates.append("password = ?")
            params.append(self.pwd_context.hash(user_update.password))
        
        if not updates:
            conn.close()
            raise ValueError("Nenhum dado para atualizar")
        
        params.append(username)
        query = f"UPDATE users SET {', '.join(updates)} WHERE username = ?"
        try:
            c.execute(query, params)
            if c.rowcount == 0:
                conn.close()
                raise ValueError("Usuário não encontrado")
            conn.commit()
            c.execute("SELECT id, username FROM users WHERE username = ?", 
                     (user_update.username if user_update.username else username,))
            updated_user = c.fetchone()
            conn.close()
            return {"id": updated_user[0], "username": updated_user[1]}
        except sqlite3.IntegrityError:
            conn.close()
            raise ValueError("Novo username já existe")

    async def delete_user(self, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("DELETE FROM users WHERE username = ?", (username,))
        if c.rowcount == 0:
            conn.close()
            raise ValueError("Usuário não encontrado")
        conn.commit()
        conn.close()

class TaskService:
    async def create_task(self, task: TaskCreate, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        if not user:
            conn.close()
            raise ValueError("Usuário não encontrado")
        user_id = user[0]
        c.execute("INSERT INTO tasks (title, description, user_id) VALUES (?, ?, ?)", 
                 (task.title, task.description, user_id))
        task_id = c.lastrowid
        conn.commit()
        conn.close()
        return task_id

    async def get_user_tasks(self, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        if not user:
            conn.close()
            raise ValueError("Usuário não encontrado")
        user_id = user[0]
        c.execute("SELECT id, title, description, user_id FROM tasks WHERE user_id = ?", (user_id,))
        tasks = [{"id": row[0], "title": row[1], "description": row[2], "user_id": row[3]} 
                 for row in c.fetchall()]
        conn.close()
        return tasks

    async def get_task(self, task_id: int, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        if not user:
            conn.close()
            raise ValueError("Usuário não encontrado")
        user_id = user[0]
        c.execute("SELECT id, title, description, user_id FROM tasks WHERE id = ? AND user_id = ?", 
                 (task_id, user_id))
        task = c.fetchone()
        conn.close()
        if task:
            return {"id": task[0], "title": task[1], "description": task[2], "user_id": task[3]}
        return None

    async def update_task(self, task_id: int, task: TaskCreate, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        if not user:
            conn.close()
            raise ValueError("Usuário não encontrado")
        user_id = user[0]
        c.execute("UPDATE tasks SET title = ?, description = ? WHERE id = ? AND user_id = ?", 
                 (task.title, task.description, task_id, user_id))
        if c.rowcount == 0:
            conn.close()
            raise ValueError("Tarefa não encontrada ou não pertence ao usuário")
        conn.commit()
        conn.close()

    async def delete_task(self, task_id: int, username: str):
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        user = c.fetchone()
        if not user:
            conn.close()
            raise ValueError("Usuário não encontrado")
        user_id = user[0]
        c.execute("DELETE FROM tasks WHERE id = ? AND user_id = ?", (task_id, user_id))
        if c.rowcount == 0:
            conn.close()
            raise ValueError("Tarefa não encontrada ou não pertence ao usuário")
        conn.commit()
        conn.close()