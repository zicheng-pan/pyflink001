#!/usr/bin/env python38
# -*- coding:utf-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableDescriptor
import os

from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.expressions import col, call
from pyflink.table.types import RowType, DataTypes
from pyflink.table.udf import udf

java8_location = "/Library/Java/JavaVirtualMachines/jdk1.8.0_333.jdk/Contents/Home"
os.environ['JAVA_HOME'] = java8_location


@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
def uppercase(charac):
    return str(charac).upper()


def hello_world():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)
    t_env.get_config().get_configuration().set_string("pipeline.jars",
                                                      "file://" + "/Users/zpan2/project/mine/pyflink001/python-flink-sql/lib/PythonApplicationDependencies.jar")
    sourceTable = t_env.from_elements([(1, "xiaowang"), (2, "xiaoli")],
                                      DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
                                                     DataTypes.FIELD("name", DataTypes.STRING())]))
    t_env.create_temporary_system_function("uppercase", uppercase)
    t_env.create_temporary_view("sourceTable", sourceTable)
    sourceTable.print_schema()
    result = sourceTable.select(col("id"), call('uppercase',col("name")))
    result.print_schema()
    t_env.create_temporary_table(
        'local_udf',
        TableDescriptor.for_connector('print')
        .schema(Schema.new_builder()
                .column('id', DataTypes.BIGINT())
                .column('_c1', DataTypes.STRING())
                .build())
        .build())

    result.execute_insert("local_udf")

if __name__ == '__main__':
    hello_world()
