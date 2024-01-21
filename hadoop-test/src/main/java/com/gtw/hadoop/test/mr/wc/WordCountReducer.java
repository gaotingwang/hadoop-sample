package com.gtw.hadoop.test.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Iterable<IntWritable> values
     * <p>
     * hello,hello,hello
     * banana,banana
     * <p>
     * 相同key经过shuffle后，会在同一个reducer中进行聚合
     * <hello,1><hello,1><hello,1> ==> <hello,<1,1,1>>
     * <banana,1><banana,1>        ==> <banana,<1,1>>
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));
    }
}
