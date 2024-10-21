# -------------------------------------------------------------------------
# AUTHOR: mohraiel matta
# FILENAME: db_connection_mongo.py
# SPECIFICATION: program that will interact with our mongodb connection in order to adjut the database in the index mongo file
# FOR: CS 4250- Assignment #2
# TIME SPENT: 12 hours spaced out,
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime


def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "DocumentsHW"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected")


def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    term_counts = {}
    terms = docText.lower().split()
    for term in terms:
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here

    terms = [
        {"term": term, "count": count, "num_chars": len(term)}
        for term, count in term_counts.items()
    ]
    # --> add your Python code here
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "data": docDate,
        "category": docCat,
        "terms": terms,
    }
    # Insert the document
    # --> add your Python code here
    col.insert_one(document)


def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})


def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    col.delete_one({"_id": docId})
    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)


def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here
    pipeline = [
        {"$unwind": "$terms"},
        {
            "$group": {
                "_id": "$terms.term",
                "documentsHW": {"$push": {"title": "$title", "count": "$terms.count"}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = col.aggregate(pipeline)
    result = {}

    for doc in results:
        term = doc["_id"]
        doc_count = ", ".join(
            [f"{entry['title']}: {entry['count']}" for entry in doc["documentsHW"]]
        )
        result[term] = doc_count

    return
