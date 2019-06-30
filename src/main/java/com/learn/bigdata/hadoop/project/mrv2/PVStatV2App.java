package com.learn.bigdata.hadoop.project.mrv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * first version
 * PV (page view)
 */
public class PVStatV2App {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PVStatV2App.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Mapper's key and value
        job.setMapOutputKeyClass(Text .class);
        job.setMapOutputValueClass(LongWritable .class);

        // Reducer's key and value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));//"input/etl"
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//"output/v2/pvstat"

        // submit job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text KEY = new Text("key");
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(KEY, ONE);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;

            for(LongWritable value : values) {
                count ++;
            }

            context.write(NullWritable.get(), new LongWritable(count));
        }
    }

}
