package com.gtw.flink.test.datastream.sink;

import com.gtw.flink.test.datastream.model.Access;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class ConsoleSinkFunction extends RichSinkFunction<Access> {

    int subTaskId;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void invoke(Access value, Context context) throws Exception {
        System.out.println(subTaskId + 1 + "gtw> " + value);
    }

}
