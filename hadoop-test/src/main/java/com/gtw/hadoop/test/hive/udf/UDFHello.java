package com.gtw.hadoop.test.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFHello extends UDF {

    public String evaluate(String msg) {
        return "Hello:" + msg;
    }

    public static void main(String[] args) {
        UDFHello udf = new UDFHello();
        System.out.println(udf.evaluate("dingjiaxiong"));
    }

}
