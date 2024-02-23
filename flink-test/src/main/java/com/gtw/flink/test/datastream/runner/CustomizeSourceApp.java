package com.gtw.flink.test.datastream.runner;

import com.gtw.flink.test.datastream.model.Access;
import com.gtw.flink.test.datastream.partition.AccessPartitioner;
import com.gtw.flink.test.datastream.source.ParallelAccessSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        source.partitionCustom(new AccessPartitioner(), x -> x.getDomain())
                .map(x -> {
                    System.out.println("current Thread is " + Thread.currentThread().getId() + ", value is " + x);
                    return x;
                });
        source.print();

        env.execute("作业名字");

    }
}
