package com.learn.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: offset, Long
 * VALUEIN: String
 *
 * the output: (word, count)
 * KEYOUT: String
 * VALUEOUT: Integer
 *
 * LongWritable, Text, Text, IntWritable are Hadoop data types instead of Java data types
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // split the given value
        String[] words = value.toString().split(" ");

        for(String word : words) {
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }


    }
}
