from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf

@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
def uppercase(charac):
    return str(charac).upper()


def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("pipeline.jars",
                           "file:///Users/zpan2/PycharmProjects/pyflink/PythonApplicationDependencies.jar")

    t_env.create_temporary_system_function("uppercase", uppercase)
    # {"a":"abc","b":123}
    source_ddl = """
            CREATE TABLE IF NOT EXISTS source_table(
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
            CREATE TEMPORARY TABLE IF NOT EXISTS sink_table(
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

    create_print = """
        CREATE TABLE IF NOT EXISTS print_table (
            a VARCHAR
        ) WITH (
          'connector' = 'print'
        );
    """
    # 创建用于展示的临时表/视图
    t_env.execute_sql(create_print)


    # 添加用于展示的数据
    insert_print = """
        insert into print_table select uppercase(a) as a from source_table
    """

    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql(insert_print)

    #{"a": "abc adsfadsf", "b": 123}
    # 处理真正的任务
    # statement_set.add_insert_sql("insert into sink_table SELECT a FROM source_table")

    statement_set.execute()



if __name__ == '__main__':
    log_processing()
