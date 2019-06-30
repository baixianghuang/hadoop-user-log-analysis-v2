package com.learn.bigdata.hadoop.project.mr;

import com.learn.bigdata.hadoop.project.utils.ContentUtils;
import com.learn.bigdata.hadoop.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageStatApp {
    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(args[0]);  // "output/v1/pagestat"
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageStatApp.class);

        job.setMapperClass(PageStatApp.MyMapper.class);
        job.setReducerClass(PageStatApp.MyReducer.class);

        // Mapper's key and value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reducer's key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));  // "input/raw/trackinfo_20130721.data"
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // "output/v1/pagestat"

        // submit job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable ONE = new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();

            Map<String, String> info = logParser.parse(log);
            String url = info.get("url");

            if (StringUtils.isNotBlank(url)) {
                String pageId = ContentUtils.getPageId(url);
                context.write(new Text(pageId), ONE);
            }
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for(LongWritable value : values) {
                count ++;
            }

            context.write(key, new LongWritable(count));
        }
    }

}
