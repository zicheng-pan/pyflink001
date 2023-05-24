package com.flink.sql.connector;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ConnecterKafkaJsonApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // {"id":1, "name":"PK", "age":30, "gender":"M", "city":"BJ"}

        /**
         * 字段：
         *      普通字段、物理字段
         *      逻辑字段：根据表达式计算出来的
         *      元数据字段：是针对Kafka中内置的属性值来获得：topic对应的partition、offset、eventtime
         */
        tableEnv.executeSql("CREATE TABLE kafka_flink (\n" +
                "id int,\n" +
                "name string,\n" +
                "age int,\n" +
                "gender string,\n" +
                "city string,\n" +
                // 逻辑字段 可以进行计算
                "new_age as age+10, \n" +
                // 元数据字段
                "event_time TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
                "offs BIGINT METADATA FROM 'offset', \n" +
                "part BIGINT METADATA FROM 'partition' \n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'test05',\n" +
                "'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'properties.group.id' = 'g12',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'json'\n" +
                ")");

        // 注意JDK版本不能过高
        tableEnv.executeSql("create table kafka_flink_city_cnt(\n"+
                //primary key(id) NOT ENFORCED 指 声明id为主键，但是不做强检验。
                "city string primary key not enforced,\n"+
                "cnt bigint\n"+
                ") WITH (\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'topic' = 'test06',\n" +
                "'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'key.format' = 'json',\n" +
                "'value.format' = 'json'\n" +
                ")");

        tableEnv.executeSql("desc kafka_flink").print();
//        tableEnv.executeSql("select * from kafka_flink").print();
        // 根据city分组，看每个城市有多少人
        // kafka connector 接入数据，然后执行统计sql，最后将结果写会kafka去
        tableEnv.sqlQuery("select city, count(1) cnt from kafka_flink group by city").executeInsert("kafka_flink_city_cnt");
//        tableEnv.executeSql("insert into kafka_flink_city_cnt select city, count(1) cnt from kafka_flink group by city");
    }

}
