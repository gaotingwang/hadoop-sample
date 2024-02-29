package com.gtw.flink.test.sql.runner;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class FlinkTableQueryApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // source
        Table table = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("user", DataTypes.STRING()),
                DataTypes.FIELD("time", DataTypes.STRING()),
                DataTypes.FIELD("url", DataTypes.STRING())
        ), row("Mary", "12:00:00", "./index"), row("Bob", "12:00:00", "./cart"), row("Mary", "12:00:00", "./prod?id=3"));
        table.printSchema();

        // transformation 按用户分组求访问次数
//        table.groupBy($("user"))
//                .select($("user"), $("url").count().as("cnt"))
//                .execute().print();

        // SQL 方式
        // createTemporaryView(name, Table)
        // createTemporaryTable(name, TableDescriptor)
        tableEnv.createTemporaryView("sourceTable", table);
        tableEnv.executeSql("select user, count(1) cnt from sourceTable group by user").print();
    }
}
