package com.gtw.flink.test.datastream.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomStateApp {

    static String path = "file:///Users/gaotingwang/Documents/IdeaProjects/demo/hadoop-sample/flink-test/chk/";
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        // 指定
        configuration.setString("execution.savepoint.path", path + "76e0159eb184a7f6077ebfffe250add0/chk-4");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        // 需要打开checkpoint，周期性生成快照，防止作业挂了后，可以从最新的checkpoint进行继续作业
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 也可以通过state.checkpoints.dir: hdfs:///checkpoints/ 进行统一指定
        env.getCheckpointConfig().setCheckpointStorage(path);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));
        env.setStateBackend(new HashMapStateBackend());

//        keyedState(env);
        operatorState(env);

        env.execute();
    }

    public static void keyedState(StreamExecutionEnvironment env) {
        // 对获取到的内容转小写，并和之前内容拼接在一起
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.keyBy(x -> "0")
                .map(new RichMapFunction<String, String>() {

                    ListState<String> listState;

                    /**
                     * 在open()中初始化 xxxState
                     * 创建xxxState需要xxxStateDescriptor(名称，类型)
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list", String.class));
                    }

                    @Override
                    public String map(String value) throws Exception {
                        // 数据更新
                        listState.add(value.toLowerCase());

                        StringBuilder sb = new StringBuilder();
                        // 结果获取
                        listState.get().forEach(sb::append);

                        return sb.toString();
                    }
                })
                .print();
    }

    public static void operatorState(StreamExecutionEnvironment env) {
        // 对获取到的内容转小写，并和之前内容拼接在一起
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(new MyOperatorStateFunction())
                .print();
    }
}
