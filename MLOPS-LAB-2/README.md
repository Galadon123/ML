# XGBoost-Ray Train ML Workflow Notebooks

This repository contains Jupyter notebooks for the machine learning workflow using **Ray** and **XGBoost**. The workflow includes data transformation, model training, and experiment tracking. This project is part of the **Poridhi Lab** (Unnamed).
The notebooks are part of the ML workflow that we're going to build and deploy with a CI/CD pipeline through github actions. The notebooks in this repository are to demonstrate the experimention processes with data transforamtion and data training along with the supporting infrastructure like feature stores, model artifacts stores.

![image](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/aa4af255-ec21-4b79-aef3-3addd115333e)

## 🔗 Overview

1. **Data Transformation**: 
   - Transforming the raw dataset using **Ray Dataset**.
   - The data transformation notebook: **1. data-transformation-and-feature-store.ipynb**
   - Storing raw data in `stagingdatastorebucket`.
   ![poridhi-notebook-banner-gif (2)](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/983f9b30-0c51-4e4d-8867-3e8e933a3467)
  

2. **Model Training**:
   - Training models with **XGBoost** integrated with **Ray Train** on a Ray cluster.
   - The training notebook: **2. training-and-saving-the-model-fixing.ipynb**
   - Storing transformed data in `featurestorebucket`.
   ![poridhi-notebook-banner-gif](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/6ddb8989-e783-4a16-be4c-b765c4c7fb7c)



3. **Experiment Tracking**:
   - Tracking experiments, models, and data using **MLflow**.
   - Storing features in `modelstorebucket` and results in `resultsstorebucket`.
     
   ![image](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/99bbac86-7c42-4e51-a6e9-0db6718d04ab)


## 🗃️ Data Stores
- **stagingdatastorebucket**: Stores raw data from various sources.
- **featurestorebucket**: Stores processed and feature-engineered data.
- **modelstorebucket**: Stores data for model training.
- **resultsstorebucket**: Stores model outputs, results, and predictions.
  
![image](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/34d60a4a-9b09-48dd-9d9d-7572eed7b81a)



## 📂 Repository Structure

- `1-raw-dataset/`: Contains raw datasets.
- `2-transformed-data/`: Contains transformed data.
- `3-training-data/`: Contains data used for training.
- `4-model-results/`: Contains model results and predictions.
- `mlartifacts/`: Contains MLflow artifacts.
- `mlruns/`: Contains MLflow run data.
- `model-repository/`: Contains saved models.


## ⤵️ Getting Started

1. **Clone the repository**:
   ```bash
   git clone https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks.git
   cd xgboost-ray-train-ml-workflow-notebooks
   ```

2. **Install dependencies (If Needed)**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the notebooks**:
   Open the Jupyter notebooks with Jupyter Lab in the environment set up on the cloud and follow the instructions provided.
   

# Set Up MLFlow Server for Experiment Tracking:

- **Install the MLFlow package in the same instance**

```bash
**pip install mlflow**
```

### Starting the MLFlow Tracking Server:

- **Start the mlflow tracking server on port 5000**

```bash
**mlflow ui --port 5000**
```

![image](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/0e21c9f9-39f6-4a0c-86c4-aef73f4f573c)


- **Port forward it to the local machine to see the mlflow server**

```bash
**ssh -i key-pair-poridhi-poc.pem -N -f -L 5000:localhost:5000 ubuntu@your-head-node's-public-ipv4-DNS**
```

![image](https://github.com/tahhnik/xgboost-ray-train-ml-workflow-notebooks/assets/25973761/2ae99119-61ba-4cc2-b97a-c206373b3a97)


- **Set the tracking URI in the training script**: 127.0.0.1:5000
  



