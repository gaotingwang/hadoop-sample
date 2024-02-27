package com.gtw.flink.test.datastream.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTTLApp {

    // wc 功能
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.map(String::toLowerCase).flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (x, collector) -> {
                    String[] splits = x.split(",");
                    for(String word : splits) {
                        collector.collect(Tuple2.of(word.trim(), 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private transient ValueState<Long> counter;

                    // 指定了state的ttl，在ttl时间内可以使用，超过ttl重新开始
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(10))
                                // 创建和写入的时候更新ttl
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                // 过期数据不可见
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("count", Long.class);
                        descriptor.enableTimeToLive(ttlConfig);
                        counter = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        counter.update(counter.value() == null ? value.f1 : counter.value() + value.f1);
                        return Tuple2.of(value.f0, counter.value());
                    }

                })
                .print();

        env.execute();
    }

}
