package com.gtw.flink.test.sql.runner;

import com.gtw.flink.test.sql.model.ClickLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableWithDataStreamApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        SingleOutputStreamOperator<ClickLog> outputStream = streamEnv.fromElements("Mary,12:00:00,./index", "Bob,12:00:00,./cart", "Mary,12:01:00,./prod?id=3")
                .map(x -> {
                    String[] splits = x.split(",");
                    return new ClickLog(splits[0].trim(), splits[1].trim(), splits[2].trim());
                });

        // 将DateStream转Table
        Table table = tableEnv.fromDataStream(outputStream);
        Table result = table.select($("user"), $("url"), $("time")).where($("user").isEqual("Mary"));

        // 将Table转DateStream
        DataStream<ClickLog> clickLogDataStream = tableEnv.toDataStream(result, ClickLog.class);
        clickLogDataStream.print();

        streamEnv.execute();
    }
}
