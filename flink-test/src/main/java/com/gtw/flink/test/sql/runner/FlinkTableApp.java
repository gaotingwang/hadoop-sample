package com.gtw.flink.test.sql.runner;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class FlinkTableApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // source
        Table table = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING())
        ), row(1, "zs"), row(2, "ls"));
        table.printSchema();

        // transformation
        Table result = table.select($("id"), $("name"));

        // sink
        tableEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).column("name", DataTypes.STRING()).build()).build());
        result.executeInsert("sinkTable");
    }
}
