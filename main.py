import queries
from models import *
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


@app.get("/api/queries/query1")
async def execute_query1(date_start: datetime, date_end: datetime):
    response = await queries.query1(date_start, date_end)
    if response:
        return response
    raise HTTPException(404, f'No response - Query 1')


@app.get("/api/queries/query2")
async def execute_query2(date_start: datetime, date_end: datetime, log_type: str):
    response = await queries.query2(date_start, date_end, log_type)
    if response:
        return response
    raise HTTPException(404, f'No response - Query 2')


@app.get("/api/queries/query3")
async def execute_query3(date: datetime):
    response = await queries.query3(date)
    if response:
        return response
    raise HTTPException(404, f'No response - Query 3')
