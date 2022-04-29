from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier

# Load the dataset
dataset = datasets.load_wine()

# Save 10, 80, and 140 for testing our model
data = [x for i, x in enumerate(dataset.data) if i not in (10, 80, 140)]
target = [x for i, x in enumerate(dataset.target) if i not in (10, 80, 140)]

# Fit the other data to a DecisionTreeClassifier
sk_classifier = DecisionTreeClassifier()
sk_classifier.fit(data, target)

# Run the model locally to test it
print("scikit-learn prediction:")
print(sk_classifier.predict(dataset.data[[10, 80, 140]]))

print("scikit-learn target:")
print(dataset.target[[10, 80, 140]])

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

# Upload the model
es_classifier = MLModel.import_model(
     es_client=es,
     model_id="wine-classifier",
     model=sk_classifier,
     feature_names=dataset.feature_names,
     es_if_exists="replace"
)
