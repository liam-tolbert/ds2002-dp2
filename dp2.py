from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json
from dotenv import load_dotenv
x = 0
y = 0
z = 0

def load_json_into_collection(filename):
    global x;
    global y;
    global z;
    # assuming you have defined a connection to your db and collection already:
    f = open(f"./data/{filename}")

    # Loading or Opening the json file
    try:
      file_data = json.load(f)
    except Exception as e:
      print(e, "error when loading", f)
      x += 1
      return
    # Inserting the loaded data in the collection
    # if JSON contains data more than one entry
    # insert_many is used else insert_one is used
    if isinstance(file_data, list):
      try:
        collection.insert_many(file_data)
      except Exception as e:
        print(e, "when importing into Mongo")
        y += 1
        return
    else:
      try:
        collection.insert_one(file_data)
      except Exception as e:
        print(e)
        y += 1
        return
    z += 1

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
    load_json_into_collection(f)

print(f"Successfully imported {z} complete documents")
print(f"{y} complete documents could not be imported")
print(f"{x} corrupted documents exist in the fileset")