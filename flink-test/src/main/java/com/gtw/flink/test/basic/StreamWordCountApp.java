package com.gtw.flink.test.basic;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用Flink进行流处理（实时）
 * 来一个数据处理一次，所以是带状态state
 */
public class StreamWordCountApp {

    public static void main(String[] args) throws Exception {
        // step0：获取上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // step1: 读取文件内容
        DataStreamSource<String> source = env.readTextFile("flink-test/data/wc.data");

        // step2: 每一行内容按照分隔符进行拆分, (pk, 1) (pk, 1) (pk, 1)
        source.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split(",");
                    for(String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)) // 使用了Java泛型，由于泛型擦除的原因，需要显式声明类型的信息
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1).print(); // step4: 按元组第一个元素分组，分组之后求和

        env.execute("作业名字");

    }
}
