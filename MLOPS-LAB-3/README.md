# Set up Ray Serve Application for Serving the Model

Follow the “3. Serving the Model with Ray Serve” notebook to set up the application script and test requests for testing the model deployment with Ray Serve.

After setting up the scripts, follow along the following instructions to deploy and the test the model for consumption,

- **Open another SSH session in the head node of the cluster**
- **Deploy the ray application which possesses the trained model with the following command,**

```bash
**serve run xgboost_test:boosting_model**
```

![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/4f5a5eeb-4533-4baa-be53-89ee0b3e23e9)

If you visit the "Serve" section of the Ray Dashboard running on your localhost's port 8265, you would see that a deployment is listed and healthy,

![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/6ca43423-1eaa-4f81-9a0e-dc9162da042f)


- **Leave the server running and open up another window. Simply run the following commands to test out the model deployment,**

```bash
**python xgboost_test_request.py**
```

It will send the http request to the sever where model is deployed (port 8000), and return the predictions, 

![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/650bedad-5848-4a13-9b38-4f1676de938c)


# Set up Monitoring for Ray Serve Deployment with Prometheus and Grafana

After deploying the model, it’s very necessary to monitor whether the model is performing well or the supporting infrastructures like networking, compute or storage are functioning properly. Ray ecosystem provides smooth integration with Prometheus and Grafana for logging, monitoring and observing the Ray Serve Deployment metrices. 

***At first we need to set up Prometheus to collect the logs and metrics.*** To set up Prometheus for your Ray cluster, you need to follow these steps:

1. Install Prometheus on your head node.
2. Configure Prometheus to scrape metrics from Ray's metrics endpoint.
3. Start Prometheus on port 9090

## Setting up Prometheus:

- ***Install Prometheus:***

```bash
**sudo apt-get update

sudo apt-get install prometheus**
```

- ***Running the Prometheus server:***

Run the Prometheus server with the configuration file given in the ray session file where the metrics export ports are defined

```bash
**sudo prometheus --config.file=/tmp/ray/session_latest/metrics/prometheus/prometheus.yml
# Default PORT: 9090**

```
![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/a18af3b9-abc9-4104-8c59-fbbe3a200082)


- ***If you need to Configure Prometheus Manually:***
    
    Create a `prometheus.yml` file with the following example content:
    
    ```bash
    **global:
      scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
      evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
    
    scrape_configs:
      - job_name: 'ray'
        metrics_path: '/metrics'
        static_configs:
        - targets: ['localhost:8000'] # Replace with the IP and port of your Ray head node.**
    ```
    
- **Port forward it to the local machine to see the mlflow server**

```bash
**ssh -i key-pair-poridhi-poc.pem -N -f -L 9090:localhost:9090 ubuntu@your-head-node's-public-ipv4-DNS**
```

![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/a5d9764f-a05a-4f58-9211-e54834530fa7)

If you go the Status → Targets, you would find the target endpoints which are referring to the metrices exported by Ray cluster and its components like the model we have deployed.

- ***Stopping the Prometheus server:***

```bash
**sudo lsof -i :9090
ps -ef | grep prometheus
sudo kill PID**
```

## Setting up Grafana for Visualizing the Metrices:

Grafana will be used to visualize the model and system metrices like the cluster utilization, node memory, node disk IO, note CPU, note network etc.

- **At first, download Grafana from the following link to your head node,** https://grafana.com/grafana/download
- **After installation, run the following command to change the ownership to acccess the bin**

```bash
**sudo chown -R ubuntu:ubuntu /usr/share/grafana #** 
```

- **Start the Grafana Server with the Ray Cluster Config**

```bash
**/usr/sbin/grafana-server --homepath=/usr/share/grafana --config=/tmp/ray/session_latest/metrics/grafana/grafana.ini web**
```

- **Now port forward to the [localhost](http://localhost) to observe the dashboards addressing the operations within Ray cluster**

![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/7228c32a-020a-4f09-965c-5a08a3344788)


You can see all the necessary metrics about the components of the Ray cluster are visualized in the default dashboard

# Testing the Model with Large Number of Requests
```bash
**python xgboost_test_request_stresstest.py**
```
![image](https://github.com/tahhnik/xgboost-model-deployment-ray-serve-local-testing/assets/25973761/be1cb215-6f3c-4caf-b54f-95e79532d57d)

