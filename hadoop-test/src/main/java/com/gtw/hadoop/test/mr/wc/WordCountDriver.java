package com.gtw.hadoop.test.mr.wc;

import com.gtw.hadoop.test.mr.format.MyOutputFormat;
import com.gtw.hadoop.test.mr.partitioner.MyPartitioner;
import com.gtw.hadoop.test.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = "hadoop-test/data/a.txt";
        String output = "out";

        // 获取Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 删除事先存在的输出目录，否则后续结果无法写入
        FileUtils.delete(configuration, output);

        // 设置运行类
        job.setJarByClass(WordCountDriver.class);

        // 设置Mapper/Reducer业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Combiner
        job.setCombinerClass(WordCountReducer.class);

        // 指定Mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置自定义分区规则
        job.setPartitionerClass(MyPartitioner.class);
        /**
         * reduce数量和分区数量的关系
         * reduce数量决定了最终文件输出的个数
         * 1. reduce 数量N = partition数量N N个文件
         * 2. reduce 数量N > partition数量M N个文件，多出来的文件为空
         * 3. reduce 数量 = 1，产生一个文件，数据合并在一起
         * 4. 1 < reduce 数量 < partition数量，报错
         */
        job.setNumReduceTasks(3);

        // 设置作业输入输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setOutputFormatClass(MyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
