package com.gtw.flink.test.datastream.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        countWindow(env);
//        tumblingWindow(env);
//        slidingWindow(env);
        sessionWindow(env);

        env.execute();
    }

    public static void countWindow(StreamExecutionEnvironment env) {
        // nc -lk 9527
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

//        source.map(x -> Integer.parseInt(x.trim()))
//                // 对于non-key, 只要窗口元素到达5个就会进行处理
//                .countWindowAll(5)
//                .sum(0)
//                .print();

        source.map(x -> {
            String[] splits = x.split(",");
            return Tuple2.of(splits[0].trim(), Integer.parseInt(splits[0].trim()));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                // 对于key，每组的元素达到个数才会触发
                .countWindow(5)
                // window function
                .sum(1)
                .print();
    }

    public static void tumblingWindow(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

//        source.map(x -> Integer.parseInt(x.trim()))
//                // 每5s一个窗口,时间到就执行
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(0)
//                .print();

        source.map(x -> {
            String[] splits = x.split(",");
            return Tuple2.of(splits[0].trim(), Integer.parseInt(splits[0].trim()));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                // 对于key，每组的元素达到时间才会触发
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
    }

    public static void slidingWindow(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(x -> Integer.parseInt(x.trim()))
                // 窗口大小10s, 滑动步长5s
                // 1: 0 ~ 5，需要注意开始滑动时，理解是从-10 ~ 0开始滑动，滑动一次到 -5 ~ 5，对应实际的就是0~5，再滑一次到0~10...
                // 2: 0 ~ 10
                // 3：5 ~ 15
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(0)
                .print();
    }

    public static void sessionWindow(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(x -> Integer.parseInt(x.trim()))
                // 5s内没有接收到数据，就会求和打印
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print();
    }
}
