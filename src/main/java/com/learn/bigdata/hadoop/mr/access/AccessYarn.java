package com.learn.bigdata.hadoop.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccessYarn {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessYarn.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        job.setPartitionerClass(AccessPartitioner.class);
        // in AccessPartitioner, divide into 3 categories ("13", "15", "other")
        job.setNumReduceTasks(3);

        // Mapper's key and value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        // Reducer's key and value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));  //"access/input"
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  //"access/output"

        // submit job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
