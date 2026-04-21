# api/main.py
from fastapi import FastAPI
from db import collection

app = FastAPI()

@app.get("/transactions")
def get_data():
    return list(collection.find({}, {"_id": 0}))