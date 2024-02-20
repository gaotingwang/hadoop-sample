package com.gtw.flink.test.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Flink进行批处理(离线)
 * 结果是一次性出来的
 */
public class BatchWordCountApp {

    public static void main(String[] args) throws Exception {
        // step0：获取上下文
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // step1: 读取文件内容
        DataSource<String> source = env.readTextFile("flink-test/data/wc.data");

        // step2: 每一行内容按照分隔符进行拆分
        source.flatMap(new FlatMapFunction<String, String>() {
            /**
             * @param value step1中读取的一行内容
             * @param out 输出
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for(String split : splits) {
                    out.collect(split.trim());
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            // step3: (pk, 1) (pk, 1) (pk, 1)
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).groupBy(0).sum(1).print(); // step4: 按元组第一个元素分组，分组之后求和


    }
}
