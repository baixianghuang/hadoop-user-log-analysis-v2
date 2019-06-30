package com.learn.bigdata.hadoop.hdfs;

public class WordCountMapper implements TestDefineMapper {
    @Override
    public void map(String line, TestDefinedContext context) {
        String[] words = line.split(" ");

        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {  // the word appears first time
                context.write(word, 1);
            }
            else {  // the word has already appeared
                int cnt = Integer.parseInt(value.toString());
                context.write(word, cnt + 1);
            }
        }

    }
}
