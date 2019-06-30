package com.learn.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccessPartitioner extends Partitioner<Text, Access> {
    @Override
    public int getPartition(Text phone, Access access, int numPartitions) {
        if(phone.toString().startsWith("13")) {
            return 0;
        } else if(phone.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
