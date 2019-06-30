package com.learn.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * define context, which act as buffer
 */
public class TestDefinedContext {
    private Map<Object, Object> cacheMap = new HashMap<Object, Object>();

    public Map<Object, Object> getHashMap(){
        return cacheMap;
    }

    /**
     * write data to cache map
     * @param key word
     * @param value the count of the word
     */
    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    public Object get(Object key) {
        return cacheMap.get(key);
    }
}
