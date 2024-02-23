package com.gtw.flink.test.datastream.runner;

import com.gtw.flink.test.datastream.model.Access;
import com.gtw.flink.test.datastream.partition.AccessPartitioner;
import com.gtw.flink.test.datastream.sink.ConsoleSinkFunction;
import com.gtw.flink.test.datastream.source.ParallelAccessSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 采用批流一体的处理方式
 */
public class CustomizeSourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

//        DataStreamSource<Access> source = env.addSource(new AccessSource());
        DataStreamSource<Access> source = env.addSource(new ParallelAccessSource());
        // 对于多并行，可以设置并行度
        source.setParallelism(3);
        // 查看并行度
        System.out.println(source.getParallelism());
        // 分区器，相同分区交给同一线程进行处理
        source.partitionCustom(new AccessPartitioner(), x -> x.getDomain())
                .map(x -> {
                    System.out.println("current Thread is " + Thread.currentThread().getId() + ", value is " + x);
                    return x;
                });

        // 分流操作
        OutputTag<Access> outputTag1 = new OutputTag<Access>("分流1"){};
        OutputTag<Access> outputTag2 = new OutputTag<Access>("分流2"){};
        // 放入不同的Tag中
        SingleOutputStreamOperator<Access> processSource = source.process(new ProcessFunction<Access, Access>() {
            @Override
            public void processElement(Access value, ProcessFunction<Access, Access>.Context ctx, Collector<Access> out) throws Exception {
                if ("qq.com".equals(value.getDomain())) {
                    ctx.output(outputTag1, value);
                } else if ("baidu.com".equals(value.getDomain())) {
                    ctx.output(outputTag2, value);
                } else {
                    out.collect(value);
                }
            }
        });

        // 从指定tag中获取
        processSource.addSink(new ConsoleSinkFunction());
        processSource.getSideOutput(outputTag1).print("1分流");
        processSource.getSideOutput(outputTag2).print("2分流");

        env.execute("作业名字");

    }
}
