package com.learn.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * read the wc.properties
 */
public class ParamsUtils {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties () {
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(getProperties().getProperty("INPUT_PATH"));
    }

}
