#!/usr/bin/env python38
#-*- coding:utf-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import os
java8_location = "/Library/Java/JavaVirtualMachines/jdk1.8.0_333.jdk/Contents/Home"
os.environ['JAVA_HOME'] = java8_location

def hello_world():
    """
    从随机Source读取数据，然后直接利用PrintSink输出。
    """
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)
    t_env.get_config().set("pipeline.jars",
                           "file:///Users/zpan2/PycharmProjects/pyflink/flink-connector-kafka-1.15.2.jar;file:///Users/zpan2/PycharmProjects/pyflink/kafka-clients-2.8.1.jar")

    t_env.execute_sql("" +
                         "create table myTable(\n" +
                         "id int,\n" +
                         "name string\n" +
                         ") with (\n" +
                         "'connector.type' = 'filesystem',\n" +
                         "'connector.path' = 'a.txt',\n" +
                         "'format.type' = 'csv'\n" +
                         ")")


    result = t_env.sql_query("select id,name from myTable where id > 1")


    t_env.execute_sql("" +
                         "create table newTable(\n" +
                         "id int,\n" +
                         "name string\n" +
                         ") with (\n" +
                         "'connector.type' = 'filesystem',\n" +
                         "'connector.path' = './target/',\n" +
                         "'format.type' = 'csv'\n" +
                         ")")

    # 注意这里需要加wait
    result.execute_insert("newTable").wait()


if __name__ == '__main__':
    hello_world()
