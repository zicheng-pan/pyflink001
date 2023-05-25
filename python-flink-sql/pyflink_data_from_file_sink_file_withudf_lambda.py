from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from myudf import myrawudf

env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = TableEnvironment.create(environment_settings=env_settings)
t_env.get_config().get_configuration().set_string('parallelism.default', '1')

t_env.get_config().set("pipeline.jars",
                       "file:///Users/zpan2/PycharmProjects/pyflink/PythonApplicationDependencies.jar")


# upper = udf(lambda i:i.upper(), [DataTypes.STRING()], DataTypes.STRING())

# @udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
# def upper(x):
#     return x.upper()

# @myrawudf.myudf
# def upper(x):
#     return x.upper()


t_env.execute_sql("""
        CREATE TABLE mySource (
          a BIGINT,
          b string
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = '/Users/zpan2/project/mine/pyflink001/data/udf_add_input.csv'
        )
    """)

t_env.execute_sql("""
        CREATE TABLE mySink01 (
          b string
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = './template/'
        )
    """)

# t_env.execute_sql("SELECT upper(b) as b FROM mySource").print()
t_env.execute_sql("insert into mySink01 SELECT upper(b) as b FROM mySource").wait()
