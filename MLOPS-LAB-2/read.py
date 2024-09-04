# ray_serve_deployment.py
from ray import serve
from starlette.requests import Request
from typing import Dict
import joblib
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.tree import DecisionTreeClassifier

@serve.deployment
class DecisionTreeModel:
    def __init__(self, model_path: str):
        self.model = joblib.load(model_path)
    
    async def __call__(self, starlette_request: Request) -> Dict:
        payload = await starlette_request.json()
        # Create feature list from payload
        feature_names = load_iris().feature_names
        features = [payload[feature] for feature in feature_names]
        # Predict
        prediction = self.model.predict([features])
        return {"prediction": int(prediction[0])}

if __name__ == "__main__":
    # Initialize Ray and Ray Serve
    import ray
    ray.init(ignore_reinit_error=True)
    serve.start()
    
    # Deploy the model
    model_path = "model.joblib"  # Path to the saved model
    decision_tree_model = DecisionTreeModel.bind(model_path)
    serve.run(decision_tree_model)
