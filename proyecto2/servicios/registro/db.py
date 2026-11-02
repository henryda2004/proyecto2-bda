import os

from pymongo import MongoClient

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
)

client = MongoClient(
    MONGO_URI,
    w="majority",
    wTimeoutMS=3000,
    tz_aware=True,
    serverSelectionTimeoutMS=2000,
)

db = client["plataforma"]