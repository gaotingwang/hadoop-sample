package com.gtw.flink.test.datastream.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 第一个泛型
 */
public class AvgAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double> {

    /**
     * 创建累加器，f0用来记录总和，f1记录总数
     */
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        // (求和，次数)
        return Tuple2.of(0, 0);
    }

    /**
     * 把进来的元素添加到累加器中，并返回一个新的累加器
     */
    @Override
    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
        System.out.println("add method invoke:" + value.f1);
        return Tuple2.of(accumulator.f0 + value.f1, accumulator.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> accumulator) {
        return Double.valueOf(accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
        return null;
    }
}
