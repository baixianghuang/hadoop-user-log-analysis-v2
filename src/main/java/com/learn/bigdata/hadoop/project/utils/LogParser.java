package com.learn.bigdata.hadoop.project.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    private Logger logger = LoggerFactory.getLogger(LogParser.class);

    public Map<String, String> parse(String log)  {

        Map<String, String> logInfo = new HashMap<>();

        IPParser ipParse = IPParser.getInstance();

        if(StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\001");  // the split "^A  " is actually \001

            String ip = splits[13];
            String country = "-";
            String province = "-";
            String city = "-";
            IPParser.RegionInfo regionInfo = ipParse.analyseIp(ip);

            if (regionInfo != null) {
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }

            logInfo.put("ip",ip);
            logInfo.put("country",country);
            logInfo.put("province",province);
            logInfo.put("city",city);

            String url = splits[1];
            logInfo.put("url", url);

            String time = splits[17];
            logInfo.put("time", time);

        }

        return logInfo;
    }

    public Map<String, String> parseV2(String log)  {

        Map<String, String> logInfo = new HashMap<String,String>();
        IPParser ipParse = IPParser.getInstance();

        if(StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\t");

            String ip = splits[0];
            String country = splits[1];
            String province = splits[2];
            String city = splits[3];
            String url = splits[4];
            String time = splits[5];
            String pageId = splits[6];

            logInfo.put("ip",ip);
            logInfo.put("country",country);
            logInfo.put("province",province);
            logInfo.put("city",city);
            logInfo.put("url",url);
            logInfo.put("time",time);
            logInfo.put("pageId",pageId);

        } else{
            logger.error("日志记录的格式不正确：" + log);
        }

        return logInfo;
    }

//    public Map<String, String> parse(String log)  {
//
//        Map<String, String> logInfo = new HashMap<String,String>();
//        IPParser ipParse = IPParser.getInstance();
//        if(StringUtils.isNotBlank(log)) {
//            String[] splits = log.split("\001");  // the split "^A  " is actually \001
//
//            String ip = splits[13];
//            String url = splits[1];
//            String sessionId = splits[10];
//            String time = splits[17];
//
//            logInfo.put("ip",ip);
//            logInfo.put("url",url);
//            logInfo.put("sessionId",sessionId);
//            logInfo.put("time",time);
//
//
//            IPParser.RegionInfo regionInfo = ipParse.analyseIp(ip);
//
//            logInfo.put("country",regionInfo.getCountry());
//            logInfo.put("province",regionInfo.getProvince());
//            logInfo.put("city",regionInfo.getCity());
//
//        } else{
//            logger.error("日志记录的格式不正确：" + log);
//        }
//
//        return logInfo;
//    }

}