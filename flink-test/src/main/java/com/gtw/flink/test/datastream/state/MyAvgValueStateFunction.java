package com.gtw.flink.test.datastream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MyAvgValueStateFunction extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    // 存放<和，个数>
    private transient ValueState<Tuple2<Long, Long>> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        })));
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Double>> collector) throws Exception {
        Tuple2<Long, Long> state = valueState.value();

        Tuple2<Long, Long> current;
        if(state != null) {
            current = state;
        }else {
            current = Tuple2.of(0L, 0L);
        }
        Tuple2<Long, Long> newState = Tuple2.of(current.f0 + value.f1, current.f1 + 1);
        valueState.update(newState);

        if(newState.f1 >= 3) {
            collector.collect(Tuple2.of(value.f0, Double.valueOf(newState.f0) / newState.f1));
            // 结果输出后，状态不需要继续保留，则可以进行清除
            valueState.clear();
        }
    }
}
