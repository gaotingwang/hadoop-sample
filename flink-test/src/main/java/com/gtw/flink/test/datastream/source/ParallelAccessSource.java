package com.gtw.flink.test.datastream.source;

import com.gtw.flink.test.datastream.model.Access;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 多并行度Source
 * 自定义数据源可用来造数据
 */
public class ParallelAccessSource implements ParallelSourceFunction<Access> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Access> sourceContext) throws Exception {
        while (isRunning) {
            // 数据不断放入sourceContext中
            sourceContext.collect(new Access(System.currentTimeMillis(), "baidu.com", new Random().nextInt(1000)));
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
