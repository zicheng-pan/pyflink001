#!/usr/bin/env python38
# -*- coding:utf-8 -*-
import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
import os

from pyflink.table.expressions import call, col
from pyflink.table.udf import udf

java8_location = "/Library/Java/JavaVirtualMachines/jdk1.8.0_333.jdk/Contents/Home"
os.environ['JAVA_HOME'] = java8_location


# 2. 注册方法
@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
# 1. 定义udf
def upper_username_func(data):
    payload = json.loads(data)
    try:
        return str(payload['work']).upper()
    except KeyError:
        return "ERR"


def hello_world():
    """
    从随机Source读取数据，然后直接利用PrintSink输出。
    """
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)
    t_env.get_config().get_configuration().set_string("pipeline.jars",
                                                      "file://" + "/Users/zpan2/PycharmProjects/pyflink/PythonApplicationDependencies.jar")
    t_env.create_temporary_system_function("upper_username_func", upper_username_func)
    t_env.execute_sql("" +
                      "create table myTable(\n" +
                      "id int,\n" +
                      "`user` string,\n" +
                      "city string \n" +
                      ") with (\n" +
                      "'connector' = 'filesystem',\n" +
                      "'path' = '/Users/zpan2/project/mine/pyflink001/python-flink-sql/b.json', \n" +
                      "'format' = 'json'\n" +
                      ")")

    t_env.execute_sql("select id,user,upper_username_func(city) from myTable")





if __name__ == '__main__':
    hello_world()
