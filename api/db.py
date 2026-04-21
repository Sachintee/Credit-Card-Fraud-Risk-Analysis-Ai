# api/db.py
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["fraudDB"]
collection = db["transactions"]