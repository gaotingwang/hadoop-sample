package com.gtw.flink.test.datastream.source;

import com.gtw.flink.test.datastream.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLSource extends RichSourceFunction<Student> {

    Connection connection;
    PreparedStatement preStatement;

    /**
     * 初始化操作，建立Connection
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
        preStatement = connection.prepareStatement("select * from student;");
    }

    @Override
    public void close() throws Exception {
        if(connection != null) {
            try {
                preStatement.close();
                connection.close();
            }finally {
                connection = null;
            }
        }
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet rs = preStatement.executeQuery();
        while (rs.next()) {
            ctx.collect(new Student(rs.getLong("id"), rs.getString("name"), rs.getInt("age")));
        }
    }

    @Override
    public void cancel() {

    }
}
