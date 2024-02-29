package com.gtw.flink.test.sql.function;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

public class FunctionApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Table table = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("age", DataTypes.INT()),
                DataTypes.FIELD("content", DataTypes.STRING())
        ), row(1, "zs", 18, "AAA,A,AA"), row(2, "ls", 25, "B,BB"), row(2, "ww", 36, "123,456,7"));
        table.printSchema();
        tableEnv.createTemporaryView("test", table);

//        scalarFunction(tableEnv);
//        aggregateFunction(tableEnv);
        tableFunction(tableEnv);
    }

    public static void scalarFunction(TableEnvironment tableEnv) {
        // function 注册
        tableEnv.createTemporaryFunction("m_upper", MyUpperUDF.class);
        // function 使用
        tableEnv.executeSql("select id, name, m_upper(name) as uname from test").print();
    }

    public static void aggregateFunction(TableEnvironment tableEnv) {
        tableEnv.createTemporaryFunction("m_agv", MyAgvUDF.class);
        tableEnv.executeSql("select m_agv(age) agv_age from test").print();
    }

    public static void tableFunction(TableEnvironment tableEnv) {
        tableEnv.createTemporaryFunction("m_split", MySplitFunction.class);
        tableEnv.executeSql("select id, name, content, word,length from test, LATERAL TABLE(m_split(content, ','))").print();
    }
}
