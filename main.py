from api import *
from fastapi import FastAPI


app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Uniswapv3 API is coming..."}

@app.get("/pools/{limit}")
async def pools(limit: int):
    dict = await call_data("pools_defillama", limit=limit)
    return {"Defillama": dict}

@app.get("/pools_data/{limit}")
async def pools_data(limit: int):
    dict = await call_data("Historical_50pools", limit=limit)
    return {"Defillama": {
        "Top50pools": dict
    }} 
