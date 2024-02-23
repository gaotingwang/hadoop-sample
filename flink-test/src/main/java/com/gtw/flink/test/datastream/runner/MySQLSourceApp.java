package com.gtw.flink.test.datastream.runner;

import com.gtw.flink.test.datastream.model.Student;
import com.gtw.flink.test.datastream.source.MySQLSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 采用批流一体的处理方式
 */
public class MySQLSourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Student> source = env.addSource(new MySQLSource());
        source.print();

        env.execute("作业名字");

    }
}
