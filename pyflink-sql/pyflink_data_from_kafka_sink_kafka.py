from pyflink.table import TableEnvironment, EnvironmentSettings


def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # specify connector and format jars
    # 1. 这里需要加运行环境依赖
    # pyflink connector https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/table/python_table_api_connectors/
    # 执行python任务同样需要和java版本一样的依赖 https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/
    t_env.get_config().set("pipeline.jars", "lib/PythonApplicationDependencies.jar")

    # {"a":"this is a content","b":100}
    source_ddl = """
            CREATE TABLE source_table(
                a VARCHAR,
                b INT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'source_topic',
              'properties.bootstrap.servers' = 'localhost:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """


    sink_ddl = """
            CREATE TABLE sink_table(
                a VARCHAR
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'sink_topic',
              'properties.bootstrap.servers' = 'localhost:9092',
              'format' = 'json'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # 2. 这里不能使用wait或者print等终结程序的指令
    t_env.sql_query("SELECT a FROM source_table") \
        .execute_insert("sink_table")


if __name__ == '__main__':
    log_processing()


# 3. 使用命令提交任务到flink执行
# flink run --python pyflink_data_from_kafka_sink_kafka.py