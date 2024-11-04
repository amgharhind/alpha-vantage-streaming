# Alpha Vantage Stock Data Pipeline

## Overview
This project is an end-to-end data pipeline that streams stock data from the **Alpha Vantage API** using **Apache Kafka**, stores it in **Apache HBase**, and processes it with **Apache Flink**. It also uses **Elasticsearch** for data querying and analytics.

---

## Table of Contents
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
---

## Technologies Used
- **Apache Kafka**: Used for data streaming between producer and consumer.
- **Apache HBase**: Used as the storage solution for processed data.
- **Apache Flink**: Processes data with SQL queries and transformations.
- **Elasticsearch**: Provides fast querying and analytics capabilities.
- **Alpha Vantage API**: Source of stock data.

---

## Project Structure
```plaintext
alpha-vantage-streaming/
├── producer_stock_data.py         # Kafka producer script to fetch data from Alpha Vantage API
├── consumer_stock_data.py         # Kafka consumer script to store data in HBase
├── hbase_stock.py                 # Script to interact with HBase for data storage
├── flink_stock.py                 # Flink job for initial SQL-based data processing
├── flink_stock2.py                # Additional Flink SQL queries and processing tasks
└── elasticsearch.py               # Elasticsearch script for indexing and querying data
