package com.gtw.flink.test.datastream.window;

import com.gtw.flink.test.datastream.function.AvgAggregateFunction;
import com.gtw.flink.test.datastream.function.OrderProcessFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowFunctionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        reduceFunction(env);
//        aggregateFunction(env);
        processFunction(env);

        env.execute();
    }

    /**
     * 求和
     * @param env
     */
    public static void reduceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(x -> Tuple2.of(1, Integer.parseInt(x.trim()))).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 增量精简计算，数据进来会立即参与计算
                .reduce((x, y) -> {
                    System.out.println("执行reduce操作：" + x + ", " + y);
                    return Tuple2.of(x.f0, x.f1 + y.f1);
                })
                .print();
    }

    /**
     * 求平均数
     */
    public static void aggregateFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(x -> {
                    String[] splits = x.split(",");
                    return Tuple2.of(splits[0].trim(), Integer.parseInt(splits[0].trim()));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 增量聚合计算，数据进来会立即参与计算
                .aggregate(new AvgAggregateFunction())
                .print();
    }

    /**
     * 拿到窗口内数据，统一进行排序
     */
    public static void processFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(x -> Integer.parseInt(x.trim()))
                .countWindowAll(5)
                // .apply()与.process() 也是一样效果
                .process(new OrderProcessFunction())
                .print().setParallelism(1);
    }

}
