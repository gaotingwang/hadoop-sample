package com.gtw.flink.test.basic;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 采用批流一体的处理方式
 */
public class FlinkWordCountApp {

    public static void main(String[] args) throws Exception {
        // step0：获取上下文，统一采用StreamExecutionEnvironment上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 本地环境，使用webUI方式
//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 8081); // webUI端口
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 动态获取传递参数工具
//        ParameterTool tool = ParameterTool.fromArgs(args);
//        String host = tool.get("host");
//        int port = tool.getInt("port");


        // 可以选择模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // step1: 读取文件内容
        DataStreamSource<String> source = env.readTextFile("flink-test/data/wc.data");

        // step2: 每一行内容按照分隔符进行拆分, (pk, 1) (pk, 1) (pk, 1)
        source.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split(",");
                    for(String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)) // 使用了Java泛型，由于泛型擦除的原因，需要显式声明类型的信息
                .keyBy(value -> value.f0)
                .sum(1).print(); // step4: 按元组第一个元素分组，分组之后求和

        env.execute("作业名字");


    }
}
