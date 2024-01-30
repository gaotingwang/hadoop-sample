package com.gtw.hadoop.test.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class UDFHelloNew extends GenericUDF {

    /**
     * 初始化
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("requires 1 argument, got " + arguments.length);
        }
        // todo 类型判断

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    /**
     * 处理业务逻辑
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String val = arguments[0].get().toString();
        return "Hello: " + val;
    }

    /**
     * 很少使用
     */
    @Override
    public String getDisplayString(String[] children) {
        return super.getStandardDisplayString("helloNew", children);
    }

}
