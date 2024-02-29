package com.gtw.flink.test.sql.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar function：将标量值转换成一个新标量值
 */
public class MyUpperUDF extends ScalarFunction {
    public String eval(String value) {
        if(value == null) {
            return null;
        }
        return value.toUpperCase();
    }
}
