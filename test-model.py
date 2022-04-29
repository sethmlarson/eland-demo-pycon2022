from sklearn import datasets

# Load the dataset
dataset = datasets.load_wine()

import os
from elasticsearch import Elasticsearch
from eland.ml import MLModel

# Authenticate with Elasticsearch
CLOUD_ID = os.environ["CLOUD_ID"]
USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(USERNAME, PASSWORD)
)

es_classifier = MLModel(
    es_client=es,
    model_id="wine-classifier"
)

print("Elasticsearch model prediction:")
print(es_classifier.predict(dataset.data[[10, 80, 140]]))
