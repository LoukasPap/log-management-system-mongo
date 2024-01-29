from models import Admin
from database import (
    fetch_one_admin,
    create_admin
)
from fastapi import FastAPI, HTTPException

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/api/admin/{name}", response_model=Admin)
async def read_item(username: str):
    response = await fetch_one_admin(username)
    if response:
        return response
    raise HTTPException(404, f'There is no admin with the name {username}')


@app.post("/api/admin/", response_model=Admin)
async def register_admin(admin: Admin):
    print(admin)
    response = await create_admin(dict(admin))
    if response:
        return response
    raise HTTPException(400, "Something went wrong")
