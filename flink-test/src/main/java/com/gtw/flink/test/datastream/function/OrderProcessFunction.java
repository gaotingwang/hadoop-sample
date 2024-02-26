package com.gtw.flink.test.datastream.function;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OrderProcessFunction extends ProcessAllWindowFunction<Integer, Integer, GlobalWindow> {

    /**
     * @param iterable 窗口内数据集合
     * @param collector 结果收集输出
     */
    @Override
    public void process(ProcessAllWindowFunction<Integer, Integer, GlobalWindow>.Context context, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
        List<Integer> list = new ArrayList<>();
        for(Integer integer : iterable) {
            list.add(integer);
        }

        list.sort(Comparator.comparingInt(o -> o));

        for(Integer integer : iterable) {
            collector.collect(integer);
        }

    }
}
