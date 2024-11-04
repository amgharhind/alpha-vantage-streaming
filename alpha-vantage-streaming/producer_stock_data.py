import requests
from kafka import KafkaProducer
from json import *

api_key = "G7QV8R0W99I560TB"
symbol = "IBM"
interval = "1min"

# Alpha Vantage API endpoint
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&outputsize=full&apikey={api_key}"

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'stock_topic'

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
}

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda K:dumps(K).encode('utf-8'))

try:
    # Make the API request
    response = requests.get(url)
    data = response.json()

    # Check if the request was successful
    if "Error Message" in data:
        print(f"Error: {data['Error Message']}")
    else:
        # Extract and send the first few data points to Kafka
        time_series_data = data["Time Series (1min)"]
        for timestamp, values in time_series_data.items():
            kafka_data = {
                'Timestamp': timestamp,
                'Open': float(values['1. open']),
                'High': float(values['2. high']),
                'Low': float(values['3. low']),
                'Close': float(values['4. close']),
                'Volume': int(values['5. volume']),
            }
            # Produce the data to Kafka topic
            producer.send(kafka_topic, value=kafka_data)
            print("Data sent to Kafka:", kafka_data)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Flush and close the producer
    producer.flush()
