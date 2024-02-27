package com.gtw.flink.test.datastream.state;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

//        keyedProcess(source);
        jobProcess(source);

        env.execute();
    }


    /**
     * 对keyed可进行process处理
     */
    private static void keyedProcess(DataStreamSource<String> source) {
        source.map(String::toLowerCase).flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (x, collector) -> {
                    String[] splits = x.split(",");
                    for(String word : splits) {
                        collector.collect(Tuple2.of(word.trim(), 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private transient ValueState<Long> counter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        counter = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                        counter.update(counter.value() == null ? value.f1 : counter.value() + value.f1);
                        collector.collect(Tuple2.of(value.f0, counter.value()));
                    }
                })
                .print();
    }

    /**
     * 整合定时任务来触发,同一个key上可以注册多个定时器，触发onTimer的执行
     * 可实现窗口功能，5s的窗口，定时器5s后触发
     */
    private static void jobProcess(DataStreamSource<String> source) {
        source.map(String::toLowerCase).flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (x, collector) -> {
                    String[] splits = x.split(",");
                    for(String word : splits) {
                        collector.collect(Tuple2.of(word.trim(), 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private transient ValueState<Long> counter;
                    final FastDateFormat format = FastDateFormat.getInstance("HH:MM:ss");;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        counter = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                        long currentTimeMillis = System.currentTimeMillis();
                        long triggerTime = currentTimeMillis + 15000;
                        System.out.println(context.getCurrentKey() + " 在"+ format.format(currentTimeMillis) +", 触发定时事件：" + format.format(triggerTime));
                        // 注册定时器
                        context.timerService().registerProcessingTimeTimer(triggerTime);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        System.out.println(ctx.getCurrentKey() + " 在" + format.format(timestamp) + "开始执行");
                    }
                })
                .setParallelism(1)
                .print();
    }

}
