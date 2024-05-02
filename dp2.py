from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json
from dotenv import load_dotenv

def load_json_into_collection(filename):
    # assuming you have defined a connection to your db and collection already:
    f = open(f"./data/{filename}")

    # Loading or Opening the json file
    try:
      file_data = json.load(f)
    except Exception as e:
      print(e, "error when loading", f)
    # Inserting the loaded data in the collection
    # if JSON contains data more than one entry
    # insert_many is used else insert_one is used
    if isinstance(file_data, list):
      try:
        collection.insert_many(file_data)
      except Exception as e:
        print(e, "when importing into Mongo")
    else:
      try:
        collection.insert_one(file_data)
      except Exception as e:
        print(e)

load_dotenv()
MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.lct4am
# specify a collection
collection = db.data

files = os.listdir("./data")
for f in files:
    print(f)
    load_json_into_collection(f)

