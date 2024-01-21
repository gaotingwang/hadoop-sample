package com.gtw.hadoop.test.mr.format;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, IntWritable> {

    /**
     * 每个Reducer结果的输出都会进去此方法
     */
    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 想要不同Partition结果写入不同文件，需要对每个reducer执行判断觉得写入文件
        String taskId = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
        String tmp = taskId.substring(taskId.lastIndexOf("_") + 1);
        Path path = new Path("out/result" + tmp +".txt");
        return new MyRecordWriter(taskAttemptContext, path);
    }

    static class MyRecordWriter extends RecordWriter<Text, IntWritable> {

        FileSystem fileSystem;
        FSDataOutputStream out;

        public MyRecordWriter(TaskAttemptContext taskAttemptContext, Path path) throws IOException {
            fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
            out = fileSystem.create(path);
        }

        @Override
        public void write(Text text, IntWritable intWritable) throws IOException, InterruptedException {
            writeObj(out, text, intWritable);
//            if (text.toString().startsWith("135")) {
//                writeObj(out1, text, intWritable);
//            } else if (text.toString().startsWith("158")) {
//                writeObj(out2, text, intWritable);
//            } else {
//                writeObj(out3, text, intWritable);
//            }
        }

        private void writeObj(FSDataOutputStream out, Text text, IntWritable intWritable) throws IOException {
            out.write(text.toString().getBytes());
            out.write(",".getBytes());
            out.write(intWritable.toString().getBytes());
            out.write("\n".getBytes());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IOUtils.closeStream(out);
            if(fileSystem != null) {
                fileSystem.close();
            }

        }
    }
}
