from pyflink.common import Configuration
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf


class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)

hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

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

    table_env.create_temporary_function("hash_code", udf(HashCode(), result_type=DataTypes.BIGINT()))
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
        insert into print_table_udf select hash_code(a) as a from source_table
    """
    t_env.execute_sql(insert_print)


if __name__ == '__main__':
    log_processing()
