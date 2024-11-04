from pyflink.table import TableEnvironment, EnvironmentSettings

# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector and format jars
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///home/hind/Desktop/flink-sql-connector-kafka-1.16.3.jar;"
    "file:///home/hind/Desktop/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"
)

# Define source table DDL
source_ddl = """
   CREATE TABLE StockData (
        `Timestamp` STRING,
        `Open` FLOAT,
        `High` FLOAT,
        `Low` FLOAT,
        `Close` FLOAT,
        `Volume` INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'stock_topic',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'None',  
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'scan.startup.mode' = 'earliest-offset'
    )
"""

# Execute DDL statement to create the source table
t_env.execute_sql(source_ddl)

# Define sink table DDL for Elasticsearch
sink_ddl = """
    CREATE TABLE ElasticsearchSink (
        `Timestamp` STRING,
        `Open` FLOAT,
        `High` FLOAT,
        `Low` FLOAT,
        `Close` FLOAT,
        `Volume` INT
    ) WITH (        
        'connector' = 'elasticsearch-7',
        'index' = 'my_data',
        'hosts' = 'localhost:9200',
        'format' = 'json'
    )
"""

# Execute DDL statement to create the Elasticsearch sink table
t_env.execute_sql(sink_ddl)

# Retrieve the source table
source_table = t_env.from_path('StockData')

# Define a SQL query to select all columns from the source table
sql_query = "SELECT * FROM StockData"

# Execute the query and retrieve the result table
result_table = t_env.sql_query(sql_query)

# Retrieve the sink table
sink_table = t_env.from_path('ElasticsearchSink')

# Insert the processed data into the Elasticsearch sink table
result_table.execute_insert('ElasticsearchSink').wait()
