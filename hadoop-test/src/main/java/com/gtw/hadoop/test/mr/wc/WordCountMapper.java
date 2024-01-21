package com.gtw.hadoop.test.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable oneCount = new IntWritable(1);

    /**
     * MapTask会对每一行输入数据调用一次map()方法
     * @param key 每行数据的偏移量
     * @param value 每行数据
     * @param context Mapper输出数据
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // 获取到的每行数据
        String hang = value.toString();
        String[] words = hang.split(",");

        //将单词输出为<单词，1>
        for (String word : words) {
            context.write(new Text(word), oneCount);
        }
    }
}
