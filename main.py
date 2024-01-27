from Admin import Admin
from database import fetch_one_admin
from fastapi import FastAPI, HTTPException

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/admin/{name}", response_model=Admin)
async def read_item(name: str):
    response = await fetch_one_admin(name)
    if response:
        return response
    raise HTTPException(404, f'There is no admin with the name {name}')
