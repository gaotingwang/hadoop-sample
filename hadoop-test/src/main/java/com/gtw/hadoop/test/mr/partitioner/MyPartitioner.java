package com.gtw.hadoop.test.mr.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String key = text.toString();
        if (key.startsWith("135")) {
            return 0;
        } else if (key.startsWith("158")) {
            return 1;
        } else {
            return 2;
        }
    }
}
