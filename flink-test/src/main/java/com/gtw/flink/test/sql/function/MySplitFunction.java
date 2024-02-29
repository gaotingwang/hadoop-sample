package com.gtw.flink.test.sql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * Table function：将标量值转换成新的行数据，列转行
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class MySplitFunction extends TableFunction<Row> {
    public void eval(String str, String delimiter) {
        for (String s : str.split(delimiter)) {
            // use collect(...) to emit a row
            collect(Row.of(s, s.length()));
        }
    }
}
