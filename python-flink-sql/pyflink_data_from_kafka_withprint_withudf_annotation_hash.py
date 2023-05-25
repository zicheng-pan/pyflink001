from pyflink.common import Configuration
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf


@udf(result_type=DataTypes.STRING())
def uppercase(self, s):
    return s.upper()

settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)
table_env.create_temporary_function("uppercase", eval)

#{"a": "testabc adsfadsf", "b": 123}
def log_processing():
    # create a streaming TableEnvironment
    config = Configuration()
    config.set_string("python.fn-execution.bundle.size", "1000")
    env_settings = EnvironmentSettings \
        .new_instance() \
        .in_streaming_mode() \
        .with_configuration(config) \
        .build()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("pipeline.jars",
                           "file:///Users/zpan2/PycharmProjects/pyflink/PythonApplicationDependencies.jar")

    # t_env.execute_sql("create temporary system function PY_UPPER as 'my_udfs.py_upper' language python")

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

    t_env.execute_sql(source_ddl)

    create_print = """
        CREATE TABLE IF NOT EXISTS print_table_udf (
            a INT
        ) WITH (
          'connector' = 'print'
        );
    """
    # 创建用于展示的临时表/视图
    t_env.execute_sql(create_print)

    # 添加用于展示的数据
    insert_print = """
        insert into print_table_udf select uppercase(a) as a from source_table
    """
    t_env.execute_sql(insert_print)


if __name__ == '__main__':
    log_processing()
