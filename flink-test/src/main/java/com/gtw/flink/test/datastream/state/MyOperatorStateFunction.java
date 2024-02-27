package com.gtw.flink.test.datastream.state;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class MyOperatorStateFunction implements MapFunction<String, String>, CheckpointedFunction {

    private transient ListState<String> checkpointState;
    FastDateFormat fastDateFormat = FastDateFormat.getInstance("HH:MM:ss");

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println(fastDateFormat.format(System.currentTimeMillis()) + ": call initializeState");
        checkpointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("list", String.class));
    }

    /**
     * 周期性更新快照
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println(fastDateFormat.format(System.currentTimeMillis()) + ": call snapshotState " + context.getCheckpointId());
    }

    @Override
    public String map(String value) throws Exception {
        System.out.println(fastDateFormat.format(System.currentTimeMillis()) + ": call map");
        if(value.contains("over")) {
            throw new RuntimeException("结束异常");
        }else {
            // 数据更新
            checkpointState.add(value.toLowerCase());

            StringBuilder sb = new StringBuilder();
            // 结果获取
            checkpointState.get().forEach(sb::append);

            return sb.toString();
        }
    }
}
