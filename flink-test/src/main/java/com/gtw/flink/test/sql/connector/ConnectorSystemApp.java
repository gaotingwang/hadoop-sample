package com.gtw.flink.test.sql.connector;

import org.apache.flink.table.api.*;

public class ConnectorSystemApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        kafkaConnectorWithTable(tableEnv);
//        fileConnectorWithSQL(tableEnv);
        mySQLConnector(tableEnv);
    }

    public static void kafkaConnectorWithTable(TableEnvironment tableEnv) {
        // kafka => table
        TableDescriptor descriptor = TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        // 普通物理字段
                        .column("name", DataTypes.STRING())
                        // 逻辑字段，根据表达式计算得出 ten_age as age + 10
                        // 元数据字段，根据kafka内置属性值来获取到 part bigint METADATA FROM 'partition'
                        .build())
                .option("topic", "test05")
                .option("properties.bootstrap.servers", "hadoop000:9092")
                .option("properties.group.id", "flink-test")
                .option("properties.startup.mode", "latest-offset")
                .option("properties.auto.offset.reset", "latest")
                // 需要flink-json包
                .format("json")
                .build();
        Table table = tableEnv.from(descriptor);
        table.printSchema();
//        table.execute().print();

        tableEnv.createTemporaryView("t_kafka_from", table);
//        tableEnv.executeSql("select name, count(1) cnt from t_kafka_from group by name").print();
        // table => kafka
        tableEnv.executeSql("CREATE TABLE t_kafka_to (\n" +
                "name STRING primary key not enforced,\n" + // sink 的 DDL 必须要有primary key
                "cnt BIGINT\n" +
                ") WITH (\n" +
                "'connector' = 'upsert-kafka',\n" + // upsert-kafka 连接器支持以 upsert 方式从 Kafka topic 中读取数据并将数据写入 Kafka topic
                "'topic' = 'test006',\n" +
                "'properties.bootstrap.servers' = 'hadoop000:9092',\n" +
                "'key.format' = 'json',\n" +
                "'value.format' = 'json'\n" +
                ")");
        //  upsert-kafka作为sink时，INSERT/UPDATE_AFTER 数据作为正常的 Kafka 消息写入
        tableEnv.executeSql("insert into t_kafka_to select name, count(1) cnt from t_kafka_from group by name");
        tableEnv.executeSql("select name, count(1) cnt from t_kafka_from group by name").print();
    }

    public static void fileConnectorWithSQL(TableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE t_csv (\n" +
                "id INT,\n" +
                "name STRING,\n" +
                "age INT\n" +
                ") WITH (\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'flink-test/data/01.csv',\n" +
                "'csv.field-delimiter' = '|',\n" +
                "'format' = 'csv'\n" +
                ")");
        tableEnv.executeSql("desc t_csv").print();
        tableEnv.executeSql("select * from t_csv").print();
    }

    public static void mySQLConnector(TableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE t_student (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'table-name' = 'student',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root'\n" +
                ");");
        tableEnv.executeSql("desc t_student").print();
        tableEnv.executeSql("select * from t_student").print();

        // 如果在 DDL 中定义了主键，JDBC sink 将使用 upsert 语义而不是普通的 INSERT 语句
    }
}
