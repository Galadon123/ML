import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Construct the input payload
sample_request_input = {
    "Year": 2020,
    "Month": 7,
    "Day": 14,
    "Hour": 15,
    "Temperature_mean": 25.5,
    "Humidity_mean": 30,
    "WindSpeed_mean": 5,
    "GeneralDiffuseFlows_mean": 200,
    "DiffuseFlows_mean": 180,
    "Weekday_first": 1,
    "IsWeekend_first": 0,
    "TimeOfDay_Afternoon_first": 1,
    "TimeOfDay_Evening_first": 0,
    "TimeOfDay_Morning_first": 0,
    "TimeOfDay_Night_first": 0,
    "Season_Autumn_first": 0,
    "Season_Spring_first": 0,
    "Season_Summer_first": 1,
    "Season_Winter_first": 0,
    "Temperature_mean_lag4": 25.0,
    "Temperature_mean_lag8": 24.5,
    "Temperature_mean_lag12": 24.0,
    "Temperature_mean_lag24": 23.5,
    "Temperature_mean_lag48": 23.0,
    "Humidity_mean_lag4": 35,
    "Humidity_mean_lag8": 40,
    "Humidity_mean_lag12": 45,
    "Humidity_mean_lag24": 50,
    "Humidity_mean_lag48": 55,
    "WindSpeed_mean_lag4": 4,
    "WindSpeed_mean_lag8": 4.5,
    "WindSpeed_mean_lag12": 5,
    "WindSpeed_mean_lag24": 5.5,
    "WindSpeed_mean_lag48": 6,
    "GeneralDiffuseFlows_mean_lag4": 190,
    "GeneralDiffuseFlows_mean_lag8": 180,
    "GeneralDiffuseFlows_mean_lag12": 170,
    "GeneralDiffuseFlows_mean_lag24": 160,
    "GeneralDiffuseFlows_mean_lag48": 150,
    "DiffuseFlows_mean_lag4": 170,
    "DiffuseFlows_mean_lag8": 160,
    "DiffuseFlows_mean_lag12": 150,
    "DiffuseFlows_mean_lag24": 140,
    "DiffuseFlows_mean_lag48": 130,
    "PowerConsumption_Zone1_sum_lag4": 1000,
    "PowerConsumption_Zone1_sum_lag8": 1050,
    "PowerConsumption_Zone1_sum_lag12": 1100,
    "PowerConsumption_Zone1_sum_lag24": 1150,
    "PowerConsumption_Zone1_sum_lag48": 1200,
    "PowerConsumption_Zone2_sum_lag4": 2000,
    "PowerConsumption_Zone2_sum_lag8": 2050,
    "PowerConsumption_Zone2_sum_lag12": 2100,
    "PowerConsumption_Zone2_sum_lag24": 2150,
    "PowerConsumption_Zone2_sum_lag48": 2200,
    "PowerConsumption_Zone3_sum_lag4": 3000,
    "PowerConsumption_Zone3_sum_lag8": 3050,
    "PowerConsumption_Zone3_sum_lag12": 3100,
    "PowerConsumption_Zone3_sum_lag24": 3150,
    "PowerConsumption_Zone3_sum_lag48": 3200,
}



# Specify the URL of your model's endpoint
url = "http://localhost:8000/"

# Define a function to send a single request
def send_request(input_data):
    response = requests.post(url, json=input_data)
    return response.text

# Number of requests to send
num_requests = 1000

# Use ThreadPoolExecutor to send requests in parallel
with ThreadPoolExecutor(max_workers=20) as executor:
    # Create a list to hold the futures
    futures = [executor.submit(send_request, sample_request_input) for _ in range(num_requests)]
    
    # Wait for all futures to complete
    for future in as_completed(futures):
        try:
            response_text = future.result()
            # Optionally print each response text or process it
            # print(response_text)
        except Exception as e:
            print(f"Request failed: {e}")

# Optionally, print a completion message or any summary statistics
print("All requests completed.")
# Specify the URL of your model's endpoint
url = "http://localhost:8000/"

for i in range(0,1000):
    # Send a POST request with the input payload as JSON
    response = requests.post(url, json=sample_request_input)

# Print the response from the server
print(response.text)