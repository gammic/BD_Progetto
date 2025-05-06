from pymongo import MongoClient
import json
client=MongoClient("mongodb://localhost:27017/")

db=client["amazonDB"]
collection=db["reviews"]
with open('Reviews.json', 'r') as f:
    reviews = json.load(f)
collection.insert_many(reviews)
