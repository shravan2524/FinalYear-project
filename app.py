from flask import Flask, request, jsonify
import pymongo 
import pandas as pd
from bson import ObjectId
from flask_cors import CORS
import json
import csv

app = Flask(__name__)
CORS(app)

uri = "mongodb+srv://shravan:ravilata@cluster0.yyer7.mongodb.net/Hackathon?retryWrites=true&w=majority"

client = pymongo.MongoClient(uri)

db = client["mydatabase"]

mycollection = db["mycollection"]
users = db["user"]

# Insert a document into the collection
# document = {"name": "John Doe", "age": 30}
# mycollection.insert_one(document)

# @app.route("/upload_csv", methods=["POST"])
# def upload_csv():
#     file = request.files["file"]
#     fileType = (request.form.get('fileType'))
#     df = pd.read_csv (file)
#     temp = df.to_json (orient='records',lines=True)
#     ret = [temp]
#     print(ret)
#     result = mycollection.insert_one({"file" : ret, "status" : "unmapped", "errors" : 0, "fileType":fileType, "fileName" : file.filename })
#     document = users.find_one({'email': 'abc@abc.com'})
#     files1 = document["files"]
#     document_id = str(result.inserted_id)
#     files1.append(document_id)
#     lresult = users.update_one({'email': 'abc@abc.com'}, {'$set': {'files': files1}})
#     return jsonify({'success': True})

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    csv_file = request.files['file']
    fileType = (request.form.get('fileType'))
    csv_reader = csv.DictReader(csv_file.read().decode('utf-8').splitlines())
    data = [row for row in csv_reader]

    json_data = json.dumps(data)

    result = mycollection.insert_one({'file': data, "status" : "unmapped", "errors" : 0, "fileType":fileType, "fileName" : csv_file.filename })
    document = users.find_one({'email': 'abc@abc.com'})
    files1 = document["files"]
    document_id = str(result.inserted_id)
    files1.append(document_id)
    lresult = users.update_one({'email': 'abc@abc.com'}, {'$set': {'files': files1}})

    return jsonify({'success': True})


@app.route("/get_file_details", methods=["POST"])
def get_file_details():
    id = (request.form.get('id'))
    print(id)
    obj_id = ObjectId(id)
    document = mycollection.find_one({'_id': obj_id})
    print(document, 'shravan')
    id = document.pop('_id')
    s = str(id)
    jsons = json.dumps(s)
    document["_id"] = s
    response = jsonify(document)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    return response


@app.route("/column_mapping", methods=["POST"])
def column_mapping():
    newcolumns = request.json["newcolumns"]
    data = request.json["data"]
    id = request.json["id"]
    print( 'shravan', data, id)
    final=[]
    fields = list(data[0].keys())
    for key in data:
        temp={}
        for field in fields:
            temp[newcolumns[field]["columnName"]] = key[field]
        final.append(temp)
    # print(final)
    obj_id = ObjectId(id)
    result = mycollection.update_one({'_id': obj_id}, {'$set': {'file': final}})
    print(result)
    response = jsonify("document")
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    return response


@app.route("/reconcile", methods=["POST"])
def reconcile():
    ids = request.json["ids"]
    fileId1 = ids[0]
    fileId2 = ids[1]
    obj_id1 = ObjectId(fileId1)
    file1 = mycollection.find_one({'_id': obj_id1})
    obj_id2 = ObjectId(fileId2)
    file2 = mycollection.find_one({'_id': obj_id2})
    data1 = file1['file']
    data2 = file2['file']
    # print(data1, data2)
    for i in range(len(data1)):
        errors = []
        for key in data1[i]:
            if(data1[i][key]!=data2[i][key]):
                errors.append(key)
        # print(errors)
        data1[i]['errors'] = errors
        data2[i]['errors'] = errors
    response = jsonify(data1, data2)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    return response


@app.route("/get_files", methods=["POST"])
def get_files():
    email =  (request.form.get('email'))
    print(email)
    document = users.find_one({'email': email})
    fileIds = document["files"]
    # print(fileIds)
    files = []
    for x in fileIds:
        document_id = ObjectId(x)
        document = mycollection.find_one({"_id": document_id})
        id = document.pop('_id')
        s = str(id)
        jsons = json.dumps(s)
        document["_id"] = s
        files.append(document)
    print(files)
    response = jsonify(files)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    return response



@app.route("/")
def hello():
    return "Hello, orld!"