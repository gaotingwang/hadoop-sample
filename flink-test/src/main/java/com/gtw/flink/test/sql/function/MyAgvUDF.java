package com.gtw.flink.test.sql.function;

import org.apache.flink.table.functions.AggregateFunction;


/**
 * aggregate function: 将多行数据里的标量值转换成一个新标量值
 */
public class MyAgvUDF extends AggregateFunction<Integer, MyAgvUDF.AvgAccum> {

    @Override
    public Integer getValue(AvgAccum avgAccum) {
        if(avgAccum.count == 0) {
            return 0;
        }
        return avgAccum.sum / avgAccum.count;
    }

    @Override
    public AvgAccum createAccumulator() {
        AvgAccum avgAccum = new AvgAccum();
        avgAccum.sum = 0;
        avgAccum.count = 0;
        return avgAccum;
    }

    /**
     * 这里数值得用包装类类型
     */
    public void accumulate(AvgAccum acc, Integer value) {
        acc.sum += value;
        acc.count += 1;
    }

    public static class AvgAccum {
        public int sum = 0;
        public int count = 0;
    }
}
