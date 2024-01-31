import queries
from models import *
from helpers import *
from fastapi import FastAPI, HTTPException

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/api/insert_log/")
async def insert_log(log_type: str, log: str):
    if log_type == "access":
        result = access_to_list(log)
    elif log_type == "dataxceiver":
        result = dataxceiver_to_list(log)
    elif log_type == "fsnamesystem":
        result = fsnamesystem_to_list(log)
    else:
        raise HTTPException(400, "Log type does not exist")

    if result == "Fail":
        raise HTTPException(400, "Bad input or wrong log type given")

    response = await queries.insert_log(log_type, result)

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


@app.get("/api/queries/query4")
async def execute_query4(date_start: datetime, date_end: datetime):
    response = await queries.query4(date_start, date_end)
    if response:
        return response
    raise HTTPException(404, f'No response - Query 4')


@app.get("/api/queries/query5")
async def execute_query5():
    response = await queries.query5()
    if response:
        return response
    raise HTTPException(404, f'No response - Query 5')


@app.get("/api/queries/query6")
async def execute_query6():
    response = await queries.query6()
    if response:
        return response
    raise HTTPException(404, f'No response - Query 6')


@app.get("/api/queries/query7")
async def execute_query7(date: datetime):
    response = await queries.query7(date)
    if response:
        return response
    raise HTTPException(404, f'No response - Query 7')


@app.get("/api/queries/query8")
async def execute_query8():
    response = await queries.query8()
    if response:
        return response
    raise HTTPException(404, f'No response - Query 8')


@app.get("/api/queries/query9")
async def execute_query9():
    response = await queries.query9()
    if response:
        return response
    raise HTTPException(404, f'No response - Query 9')


@app.get("/api/queries/query10")
async def execute_query10():
    response = await queries.query10()
    if response:
        return response
    raise HTTPException(404, f'No response - Query 10')


@app.get("/api/queries/query11")
async def execute_query11(username: str):
    response = await queries.query11(username)
    if response:
        return response
    raise HTTPException(404, f'No response - Query 11')
