package com.gtw.flink.test.datastream.runner;

import com.gtw.flink.test.datastream.model.Access;
import com.gtw.flink.test.datastream.partition.AccessPartitioner;
import com.gtw.flink.test.datastream.sink.ConsoleSinkFunction;
import com.gtw.flink.test.datastream.source.ParallelAccessSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

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
        // 终端 sink
        processSource.addSink(new ConsoleSinkFunction());

        // 文件 sink
        processSource.getSideOutput(outputTag1).addSink(StreamingFileSink.<Access>forRowFormat(new Path("out"), new SimpleStringEncoder<>())
                .withRollingPolicy(DefaultRollingPolicy.builder() // 文件滚动策略
                        .withRolloverInterval(Duration.ofMinutes(15)) // 按时间间隔滚
                        .withInactivityInterval(Duration.ofMinutes(5)) // 按不活跃间隔
                        .withMaxPartSize(MemorySize.ofMebiBytes(1024)) // 按大小滚
                        .build())
                .build());

        // redis sink
//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
//                .setHost("localhost")
//                        .setPort(6379)
//                                .setDatabase(1)
//                                        .build();
//        processSource.getSideOutput(outputTag2).addSink(new RedisSink<Access>(config, new RedisMapper<Access>(){
//
//            @Override
//            public RedisCommandDescription getCommandDescription() {
//                return null;
//            }
//
//            @Override
//            public String getKeyFromData(Access access) {
//                return null;
//            }
//
//            @Override
//            public String getValueFromData(Access access) {
//                return null;
//            }
//
//
//        }));

        // MySQL sink
        processSource.getSideOutput(outputTag2).addSink(JdbcSink.sink(
                "insert into t_access (time, domain, traffic) values (?, ?, ?) on duplicate key update traffic = ?",
                (statement, access) -> {
                    statement.setLong(1, access.getTime());
                    statement.setString(2, access.getDomain());
                    statement.setDouble(3, access.getTraffic());
                    statement.setDouble(4, access.getTraffic());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(3)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute("作业名字");

    }
}
