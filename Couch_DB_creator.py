import json
import requests
import math


def sanitize(doc):
    if isinstance(doc, dict):
        return {k: sanitize(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [sanitize(v) for v in doc]
    elif isinstance(doc, float):
        if math.isinf(doc) or math.isnan(doc):
            return None  # oppure 0
    return doc


with open('Reviews.json') as f:
    data = json.load(f)

# Pulizia
cleaned_data = [sanitize(doc) for doc in data]

# URL di CouchDB
url = 'http://gammic:marco2002@127.0.0.1:5984/amazon_reviews/_bulk_docs'

batch_size = 1000
for i in range(0, len(cleaned_data), batch_size):
    chunk = {"docs": cleaned_data[i:i + batch_size]}
    response = requests.post(url, json=chunk)

    if response.status_code == 201:
        print(f"Batch {i // batch_size + 1}: Success")
    else:
        print(f"Batch {i // batch_size + 1}: Failed with status {response.status_code}")
        print(response.text)
