package com.learn.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Driver: configure properties of Mapper and Reducer
 */
public class WordCountApp {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "ada");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:8020");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountApp.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Mapper's key and value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer's key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // delete the output files if they already exist
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:8020"), configuration, "ada");
        Path outputPath = new Path("/wordcount/output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        // submit job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }

}
