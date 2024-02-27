package com.gtw.flink.test.datastream.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class ValueStateApp {

    /**
     * 相同key元素每达到3个，开始求平均值
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Long>> list = new ArrayList<>();
        list.add(Tuple2.of("1", 1L));
        list.add(Tuple2.of("1", 2L));
        list.add(Tuple2.of("2", 3L));
        list.add(Tuple2.of("1", 4L));
        list.add(Tuple2.of("2", 5L));
        list.add(Tuple2.of("2", 6L));
        list.add(Tuple2.of("1", 7L));
        list.add(Tuple2.of("1", 8L));
        list.add(Tuple2.of("1", 9L));

        env.fromCollection(list)
            .keyBy(x -> x.f0)
            .flatMap(new MyAvgValueStateFunction())
            .print();

        env.execute();
    }

}
