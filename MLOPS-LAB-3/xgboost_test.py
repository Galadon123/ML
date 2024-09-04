from ray import serve
import pandas as pd

import pickle
import json
import numpy as np
import os
import tempfile
from starlette.requests import Request
from typing import Dict
import ubjson

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import mean_squared_error
import xgboost as xgb

@serve.deployment
class BoostingModel:
    # def __init__(self, model_path: str):
    #     with open(model_path, "rb") as f:
    #         self.model = pickle.load(f)
    def __init__(self, model_path: str):
        self.model = xgb.Booster()
        self.model.load_model(model_path)

    async def __call__(self, starlette_request: Request) -> Dict:
        payload = await starlette_request.json()
        print("Worker: received starlette request with data", payload)

        input_vector = [
            payload["Year"],
            payload["Month"],
            payload["Day"],
            payload["Hour"],
            payload["Temperature_mean"],
            payload["Humidity_mean"],
            payload["WindSpeed_mean"],
            payload["GeneralDiffuseFlows_mean"],
            payload["DiffuseFlows_mean"],
            payload["Weekday_first"],
            payload["IsWeekend_first"],
            payload["TimeOfDay_Afternoon_first"],
            payload["TimeOfDay_Evening_first"],
            payload["TimeOfDay_Morning_first"],
            payload["TimeOfDay_Night_first"],
            payload["Season_Autumn_first"],
            payload["Season_Spring_first"],
            payload["Season_Summer_first"],
            payload["Season_Winter_first"],
            payload["Temperature_mean_lag4"],
            payload["Temperature_mean_lag8"],
            payload["Temperature_mean_lag12"],
            payload["Temperature_mean_lag24"],
            payload["Temperature_mean_lag48"],
            payload["Humidity_mean_lag4"],
            payload["Humidity_mean_lag8"],
            payload["Humidity_mean_lag12"],
            payload["Humidity_mean_lag24"],
            payload["Humidity_mean_lag48"],
            payload["WindSpeed_mean_lag4"],
            payload["WindSpeed_mean_lag8"],
            payload["WindSpeed_mean_lag12"],
            payload["WindSpeed_mean_lag24"],
            payload["WindSpeed_mean_lag48"],
            payload["GeneralDiffuseFlows_mean_lag4"],
            payload["GeneralDiffuseFlows_mean_lag8"],
            payload["GeneralDiffuseFlows_mean_lag12"],
            payload["GeneralDiffuseFlows_mean_lag24"],
            payload["GeneralDiffuseFlows_mean_lag48"],
            payload["DiffuseFlows_mean_lag4"],
            payload["DiffuseFlows_mean_lag8"],
            payload["DiffuseFlows_mean_lag12"],
            payload["DiffuseFlows_mean_lag24"],
            payload["DiffuseFlows_mean_lag48"],
            payload["PowerConsumption_Zone1_sum_lag4"],
            payload["PowerConsumption_Zone1_sum_lag8"],
            payload["PowerConsumption_Zone1_sum_lag12"],
            payload["PowerConsumption_Zone1_sum_lag24"],
            payload["PowerConsumption_Zone1_sum_lag48"],
            payload["PowerConsumption_Zone2_sum_lag4"],
            payload["PowerConsumption_Zone2_sum_lag8"],
            payload["PowerConsumption_Zone2_sum_lag12"],
            payload["PowerConsumption_Zone2_sum_lag24"],
            payload["PowerConsumption_Zone2_sum_lag48"],
            payload["PowerConsumption_Zone3_sum_lag4"],
            payload["PowerConsumption_Zone3_sum_lag8"],
            payload["PowerConsumption_Zone3_sum_lag12"],
            payload["PowerConsumption_Zone3_sum_lag24"],
            payload["PowerConsumption_Zone3_sum_lag48"],
        ]
        column_names = [
            "Year", "Month", "Day", "Hour", "Temperature_mean", "Humidity_mean", "WindSpeed_mean",
            "GeneralDiffuseFlows_mean", "DiffuseFlows_mean", "Weekday_first", "IsWeekend_first",
            "TimeOfDay_Afternoon_first", "TimeOfDay_Evening_first", "TimeOfDay_Morning_first",
            "TimeOfDay_Night_first", "Season_Autumn_first", "Season_Spring_first", "Season_Summer_first",
            "Season_Winter_first", "Temperature_mean_lag4", "Temperature_mean_lag8",
            "Temperature_mean_lag12", "Temperature_mean_lag24", "Temperature_mean_lag48",
            "Humidity_mean_lag4", "Humidity_mean_lag8", "Humidity_mean_lag12", "Humidity_mean_lag24",
            "Humidity_mean_lag48", "WindSpeed_mean_lag4", "WindSpeed_mean_lag8", "WindSpeed_mean_lag12",
            "WindSpeed_mean_lag24", "WindSpeed_mean_lag48", "GeneralDiffuseFlows_mean_lag4",
            "GeneralDiffuseFlows_mean_lag8", "GeneralDiffuseFlows_mean_lag12",
            "GeneralDiffuseFlows_mean_lag24", "GeneralDiffuseFlows_mean_lag48", "DiffuseFlows_mean_lag4",
            "DiffuseFlows_mean_lag8", "DiffuseFlows_mean_lag12", "DiffuseFlows_mean_lag24",
            "DiffuseFlows_mean_lag48", "PowerConsumption_Zone1_sum_lag4", "PowerConsumption_Zone1_sum_lag8",
            "PowerConsumption_Zone1_sum_lag12", "PowerConsumption_Zone1_sum_lag24",
            "PowerConsumption_Zone1_sum_lag48", "PowerConsumption_Zone2_sum_lag4",
            "PowerConsumption_Zone2_sum_lag8", "PowerConsumption_Zone2_sum_lag12",
            "PowerConsumption_Zone2_sum_lag24", "PowerConsumption_Zone2_sum_lag48",
            "PowerConsumption_Zone3_sum_lag4", "PowerConsumption_Zone3_sum_lag8",
            "PowerConsumption_Zone3_sum_lag12", "PowerConsumption_Zone3_sum_lag24",
            "PowerConsumption_Zone3_sum_lag48"
        ]

        # Create DataFrame
        df_input_vector = pd.DataFrame([input_vector], columns=column_names)
        
        # Pass DataFrame to xgb.DMatrix
        dmat_input = xgb.DMatrix(df_input_vector)

        # dmat_input = xgb.DMatrix([input_vector])
        prediction = self.model.predict(dmat_input)[0]
        return {"result": prediction}

# MODEL_PATH = "model.ubj"
# MODEL_PATH = "/home/ubuntu/model-training-directory/model.pkl"
MODEL_PATH = "/model-repository/model.ubj"

boosting_model = BoostingModel.bind(MODEL_PATH)
