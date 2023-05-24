package com.flink.sql.connector;

import org.apache.flink.table.api.*;

public class ConnecterFileSystemJsonApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
//
//        Schema schema = Schema.newBuilder()
//                .column("id", DataTypes.INT())
//                .column("user", DataTypes.STRING())
//                .column("city", DataTypes.ROW(
//                        DataTypes.FIELD("work", DataTypes.STRING()),
//                        DataTypes.FIELD("home", DataTypes.STRING())
//                ))
//                .build();
//
//        tableEnv.createTemporaryTable("pk_json",
//                TableDescriptor.forConnector("filesystem")
//                        .schema(schema)
//                .option("path", "data/json/02.json")
//                .format("json")
//                .build()
//        );
//
//        tableEnv.executeSql("desc pk_json").print();
//        tableEnv.executeSql("select * from pk_json").print();


        tableEnv.executeSql("create table pk_json2(\n" +
                "id int,\n" +
                // 这里因为user在sql中是关键字所以使用`
                "`user` string,\n" +
                // 这里定义内部数据结构
                // 也可以使用 city map<string,string> 取值的方式一样
                "city map<string, string> \n" +
//                "city row<work string, home string> \n" +
                ") with (\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'data/json/02.json', \n" +
                "'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("desc pk_json2").print();
        tableEnv.executeSql("select * from pk_json2").print();
        tableEnv.executeSql("select id, user, city['work'] work_city, city['home'] home_city from pk_json2").print();

    }

}
