package com.gtw.hadoop.test.mr.join;

import com.gtw.hadoop.test.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class MapJoin {

    public static void main(String[] args) throws Exception {
        String input = "hadoop-test/data/join/emp.data";
        String output = "out/join";

        // 获取Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 删除事先存在的输出目录，否则后续结果无法写入
        FileUtils.delete(configuration, output);

        // 设置运行类
        job.setJarByClass(MapJoin.class);

        // 设置Mapper
        job.setMapperClass(MyMapper.class);

        // 指定Mapper输出数据的kv类型
        job.setMapOutputKeyClass(JoinInfo.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 加载小表数据
        job.setCacheFiles(Collections.singletonList(new URI("hadoop-test/data/join/org.data")).toArray(new URI[0]));
        // 此处不需要reduce
        job.setNumReduceTasks(0);

        // 设置作业输入输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, JoinInfo, NullWritable> {

        private final Map<String , String> cacheMap = new HashMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, JoinInfo, NullWritable>.Context context) throws IOException {
            URI[] files = context.getCacheFiles();
            String path = files[0].getPath();
            BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(path)), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null){
                String[] splits = line.split("\t");
                cacheMap.put(splits[0] , splits[1]);
            }

            IOUtils.closeStreams(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, JoinInfo, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            JoinInfo info = new JoinInfo(splits[0] , splits[1] , splits[2] , "");

            // 关联数据从缓存的小表中获取
            info.setOrgName(cacheMap.getOrDefault(info.getOrgCode(), "-"));

            context.write(info , NullWritable.get());
        }
    }

}
