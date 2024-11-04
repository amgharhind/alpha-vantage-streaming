from kafka import KafkaConsumer
from json import loads
import happybase
import struct

# Create a Kafka consumer
consumer = KafkaConsumer(
    'stock_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to HBase
connection = happybase.Connection('localhost', port=9090)

# Create the table if it doesn't exist
table_name = 'stock_table'
column_family = 'cf1'
if table_name.encode('utf-8') not in connection.tables():
    connection.create_table(
        table_name,
        {column_family: dict()},
    )

# Open the table
table = connection.table(table_name)

# Process incoming messages
for message in consumer:
    try:
        data = message.value
        print("Received data:", data)

        # Assuming data is a dictionary with the necessary fields
        row_key = str(data['Timestamp'])  # Ensure row_key is a string

        # Convert float values to binary representation
        column_data = {
            "{}:Volume".format(column_family): struct.pack('!f', float(data['Volume'])),
            "{}:Timestamp".format(column_family): str(data['Timestamp']).encode('utf-8'),
            "{}:High".format(column_family): struct.pack('!f', float(data['High'])),
            "{}:Low".format(column_family): struct.pack('!f', float(data['Low'])),
            "{}:Close".format(column_family): struct.pack('!f', float(data['Close'])),
            "{}:Open".format(column_family): struct.pack('!f', float(data['Open']))
        }

        # Store data in HBase
        table.put(str(row_key).encode('utf-8'), column_data)
        print("Data stored in HBase:", data)

    except Exception as e:
        print("Error processing message:", str(e))

# Close the connection to HBase
connection.close()
