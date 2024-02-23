package com.gtw.flink.test.datastream.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 相同分区内，执行的线程ID是相同的
 */
public class AccessPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int partitionNum) {
        if("qq.com".equals(key)) {
            return 0;
        }else if("baidu.com".equals(key)) {
            return 1;
        }else {
            return 2;
        }
    }
}
