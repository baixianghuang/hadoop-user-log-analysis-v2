package com.learn.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * only to implement word count (no map reduce)
 * step 1: read the file in HDFS  (using HDFS API)
 * step 2: count the number of words in each line (Map)
 * step 3: buffer the obtained result (Context)
 * step 4: output the result (using HDFS API)
 */
public class HDFSWCApp01 {
    public static void main(String[] args) throws Exception{

        // step 1: read the file in HDFS  (using HDFS API)
        Path input = new Path("/hdfsapi/test/README.txt");

        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration(), "ada");

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        TestDefineMapper mapper = new WordCountMapper();
        TestDefinedContext context = new TestDefinedContext();

        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {
                // TODO step 2: count the number of words in each line (Map)
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }

        // TODO step 3: buffer the obtained result Map
        Map<Object, Object> contextMap = context.getHashMap();


        // step 4: output the result (using HDFS API)
        Path output = new Path("/hdfsapi/output/");
        FSDataOutputStream out = fs.create(new Path(output, new Path("wc.out")));

        //TODO output the content in the buffer
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();

        for(Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());

        }

        out.close();
        fs.close();
        System.out.println("success");
    }



}
