package com.gtw.flink.test.datastream.window;

import com.gtw.flink.test.datastream.model.Access;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WaterMarkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Watermark
//        tumblingWaterMark(env);
//        slidingWaterMark(env);

        // 延迟乱序数据处理
//        durationWaterMark(env);
//        latenessWaterMark(env);
        sideOutWaterMark(env);

        env.execute();
    }

    /**
     * 求窗口内每个domain出现的次数
     * [window_start, window_end) 5s时间窗口，ms时间戳
     * 第一次录入 1000,a,1，时间窗口大小为：
     * [0, 5000) 当录入5000,a,1，waterMark = 4999，当waterMark >= window_end - 1，就会触发窗口内的执行
     * [5000, 10000) 录入>=10000,a,1 时触发窗口执行
     */
    public static void tumblingWaterMark(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(value -> {
                    String[] str = value.split(",");
                    return new Access(Long.parseLong(str[0].trim()), str[1].trim(), Double.parseDouble(str[2].trim()));
                })
                // Watermark 指定
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Access>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Access>() {
                            // 指定 event-time
                            @Override
                            public long extractTimestamp(Access access, long l) {
                                return access.getTime();
                            }
                        })
                )
                .process(new ProcessFunction<Access, Access>() {
                    @Override
                    public void processElement(Access access, ProcessFunction<Access, Access>.Context context, Collector<Access> collector) throws Exception {
                        long waterMark = context.timerService().currentWatermark();
                        // 这里打印出来的实际是上次的waterMark
                        System.out.println("该数据value is :" + access + ", mk is :" + waterMark);
                        collector.collect(access);
                    }
                })
                .keyBy(Access::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("traffic")
                .print();
    }

    /**
     * 滑动窗口，窗口大小6s，滑动步长2s
     * 产生窗口：
     * [-6, 0)
     * [-4, 2) => [0, 2)
     * [-2, 4) => [0, 4)
     * [0, 6) => [0, 6)
     * [2, 8) => [2, 8)
     *
     *
     * 1000,a,1
     * 1999,a,1
     * 2000,a,1
     * Access{domain='a', traffic=2.0} 对应的是[0, 2)窗口
     *
     * 2222,b,1
     * 2888,a,1
     * 4000,a,1
     * Access{domain='a', traffic=4.0} 对应的是[0, 4)窗口
     * Access{domain='b', traffic=1.0}
     *
     * 4888,a,1
     * 6000,a,1
     * Access{time=1000, domain='a', traffic=6.0} 对应的是[0, 6)窗口
     * Access{time=2222, domain='b', traffic=1.0}
     */
    public static void slidingWaterMark(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(value -> {
                    String[] str = value.split(",");
                    return new Access(Long.parseLong(str[0].trim()), str[1].trim(), Double.parseDouble(str[2].trim()));
                })
                // Watermark 指定
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Access>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Access>() {
                            // 指定 event-time
                            @Override
                            public long extractTimestamp(Access access, long l) {
                                return access.getTime();
                            }
                        })
                )
                .keyBy(Access::getDomain)
                .window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)))
                .sum("traffic")
                .print();
    }


    /**
     * 支持容忍度，延迟窗口触发条件
     * 1000,a,1
     * 1999,a,1
     * 5000,b,1 ---> 本来是在这里触发窗口执行，延迟2s到7000时才执行，这样可以让延迟到来的4000数据可以加入前窗口执行
     * 4000,a,1
     * 7000,c,1
     * Access{domain='a', traffic=3.0}
     */
    public static void durationWaterMark(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(value -> {
                    String[] str = value.split(",");
                    return new Access(Long.parseLong(str[0].trim()), str[1].trim(), Double.parseDouble(str[2].trim()));
                })
                // Watermark 指定
                .assignTimestampsAndWatermarks(
                        // forBoundedOutOfOrderness，可以传入容忍度，减慢窗口触发条件，延迟2s后触发
                        // 相当于到第7秒，才会触发前5s的窗口内容执行
                        WatermarkStrategy.<Access>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Access>() {
                            // 指定 event-time
                            @Override
                            public long extractTimestamp(Access access, long l) {
                                return access.getTime();
                            }
                        })
                )
                .keyBy(Access::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("traffic")
                .print();
    }

    /**
     * 允许延迟方式
     * 1000,a,1
     * 1999,a,1
     * 5000,b,1
     * Access{domain='a', traffic=2.0}
     *
     * 4000,a,1
     * Access{time=1000, domain='a', traffic=3.0}
     *
     * 4999,a,1
     * Access{time=1000, domain='a', traffic=4.0}
     *
     * 7000,c,1
     * 4300,a,1 --------->大于2s后的数据，被舍弃了
     * 8000,a,1
     * 10000,c,1
     * Access{domain='c', traffic=1.0}
     * Access{domain='a', traffic=1.0}
     * Access{domain='b', traffic=1.0}
     */
    public static void latenessWaterMark(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(value -> {
                    String[] str = value.split(",");
                    return new Access(Long.parseLong(str[0].trim()), str[1].trim(), Double.parseDouble(str[2].trim()));
                })
                // Watermark 指定
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Access>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Access>() {
                            // 指定 event-time
                            @Override
                            public long extractTimestamp(Access access, long l) {
                                return access.getTime();
                            }
                        })
                )
                .keyBy(Access::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 允许延迟2s
                .allowedLateness(Time.seconds(2))
                .sum("traffic")
                .print();
    }

    /**
     * 边路输出延迟数据
     * 1000,a,1
     * 1999,a,1
     * 5000,b,1
     * Access{domain='a', traffic=2.0}
     *
     * 4000,a,1
     * late data is :> Access{domain='a', traffic=1.0}
     *
     * 4999,a,1
     * late data is :> Access{domain='a', traffic=1.0}
     *
     * 7000,c,1
     * 4300,a,1
     * late data is :> Access{domain='a', traffic=1.0}
     *
     * 8000,a,1
     * 10000,c,1
     * Access{domain='c', traffic=1.0}
     * Access{domain='a', traffic=1.0}
     * Access{domain='b', traffic=1.0}
     */
    public static void sideOutWaterMark(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        OutputTag<Access> outputTag = new OutputTag<Access>("late data"){};

        SingleOutputStreamOperator<Access> result = source.map(value -> {
                    String[] str = value.split(",");
                    return new Access(Long.parseLong(str[0].trim()), str[1].trim(), Double.parseDouble(str[2].trim()));
                })
                // Watermark 指定
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Access>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Access>() {
                            // 指定 event-time
                            @Override
                            public long extractTimestamp(Access access, long l) {
                                return access.getTime();
                            }
                        })
                )
                .keyBy(Access::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 允许延迟2s
                .sideOutputLateData(outputTag)
                .sum("traffic");

        DataStream<Access> sideOutput = result.getSideOutput(outputTag);
        sideOutput.print("late data is :");

        result.print();
    }
}
