package com.gtw.hadoop.test.mr.join;

import com.gtw.hadoop.test.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoin {

    public static void main(String[] args) throws Exception {
        String input = "hadoop-test/data/join";
        String output = "out/join";

        // 获取Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 删除事先存在的输出目录，否则后续结果无法写入
        FileUtils.delete(configuration, output);

        // 设置运行类
        job.setJarByClass(ReduceJoin.class);

        // 设置Mapper/Reducer业务类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 指定Mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinInfo.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(JoinInfo.class);

        // 设置作业输入输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, JoinInfo> {

        private String fileName;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, JoinInfo>.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            fileName = fileSplit.getPath().toString();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinInfo>.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            JoinInfo info;
            if(fileName.contains("emp")){
                info = new JoinInfo(splits[0] , splits[1] , splits[2] , "emp");
                info.setOrgName("?");
            }else{
                info = new JoinInfo(splits[0], splits[1] , "org");
                info.setUserId("-");
                info.setUserName("-");
            }
            context.write(new Text(info.getOrgCode()) , info);
        }
    }

    public static class MyReducer extends Reducer<Text, JoinInfo, NullWritable, JoinInfo> {
        @Override
        protected void reduce(Text key, Iterable<JoinInfo> values, Reducer<Text, JoinInfo, NullWritable, JoinInfo>.Context context) throws IOException, InterruptedException {
            String orgName = null;
            List<JoinInfo> list = new ArrayList<>();
            for(JoinInfo info : values){
                if("emp".equals(info.getFlag())){
                    //一定要新new一个对象，否则出问题
                    //内存指针指向问题
                    list.add(new JoinInfo(info.getUserId() , info.getUserName() , info.getOrgCode() , ""));
                }else{
                    orgName = info.getOrgName();
                }
            }

            for(JoinInfo info : list){
                info.setOrgName(orgName);
                context.write(NullWritable.get() , info);
            }
        }
    }
}
