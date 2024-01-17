package com.gtw.hadoop.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;

public class HDFSAPITest01 {

    FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "Tingwang.Gao");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
//        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.replication", "1");

        fileSystem = FileSystem.get(configuration);
    }

    @After
    public void shutDown() throws Exception {
        if(null != fileSystem) {
            fileSystem.close();
        }
    }

    @Test
    public void testMkdir() throws Exception {
        String path = "/hdfs/api/test";
        if (!fileSystem.exists(new Path(path))) {
            // 创建文件夹
            fileSystem.mkdirs(new Path(path));
        }
    }

    /**
     * 上传
     */
    @Test
    public void testPut() throws Exception {
        fileSystem.copyFromLocalFile(new Path("data/a.txt"), new Path("/hdfs/api/test/aa.txt"));
    }

    /**
     * IO流上传
     */
    @Test
    public void testCopyFromLocalIO() throws Exception {
        BufferedInputStream in = new BufferedInputStream(Files.newInputStream(new File("data/a.txt").toPath()));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs/api/test/aa-io.txt"));

        IOUtils.copyBytes(in, out, 4096);

        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
    }

    /**
     * 下载
     */
    @Test
    public void testGet() throws Exception {
        fileSystem.copyToLocalFile(new Path("/hdfs/api/test/aa.txt"), new Path("data/b.txt"));
    }

    /**
     * IO流下载
     */
    @Test
    public void testCopyToLocalIO() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfs/api/test/aa-io.txt"));
        FileOutputStream out = new FileOutputStream("data/b.txt");


        IOUtils.copyBytes(in, out, 4096);

        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
    }

    /**
     * 列表查询
     */
    @Test
    public void testList() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfs/api/test"), true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            // 副本信息
            short replication = fileStatus.getReplication();
            // 权限
            String permission = fileStatus.getPermission().toString();
            // 大小
            long length = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    @Test
    public void testDelete() throws Exception {
        fileSystem.delete(new Path("/hdfs/api/"), true);
    }

}
