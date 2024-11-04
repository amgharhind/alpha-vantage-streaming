
from kafka import KafkaConsumer
from json import loads

# Create a Kafka consumer
consumer = KafkaConsumer(
    'stock_topic',  # Topic to consume messages from
    bootstrap_servers=['localhost:9092'],  # Kafka server addresses
    auto_offset_reset='earliest',  # Reset offset to the earliest available message
    #auto_offset_reset='latest',    # Reset offset to the latest available message available message

    enable_auto_commit=True,  # Enable auto commit of consumed messages
    group_id=None,  # Consumer group ID (None indicates an individual consumer)
    value_deserializer=lambda x: loads(x.decode('utf-8'))  # Deserialize the message value from JSON to Python object
)
print('here')
# Process incoming messages
for message in consumer:
    try:
        consumer.poll()

        data = message.value  # Get the value of the message (data)
        print(data)  # Print the data using standard print statement
    except Exception as e:
        print(f"Error processing message: {e}")