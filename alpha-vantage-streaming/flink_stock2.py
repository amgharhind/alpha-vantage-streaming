from pyflink.table import TableEnvironment, EnvironmentSettings

# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector and format jars
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///home/hind/Desktop/flink-sql-connector-kafka-1.16.3.jar"
)

# Define source table DDL
source_ddl = """
   CREATE TABLE StockData (
        `Timestamp` TIMESTAMP(3),
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

# Define a SQL query to find the day with the highest closing value
sql_query = """
 SELECT
  distinct(DATE_FORMAT(`Timestamp`, 'yyyy-MM-dd'))   AS trading_day,
    MAX(`Close`) AS max_close
FROM StockData
GROUP BY DATE_FORMAT(`Timestamp`, 'yyyy-MM-dd')
   
"""

# Execute the query and retrieve the result table
result_table = t_env.sql_query(sql_query)

# Print the result table to the console
result_table.execute().print()
