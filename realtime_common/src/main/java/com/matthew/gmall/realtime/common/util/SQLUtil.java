package com.matthew.gmall.realtime.common.util;

import com.matthew.gmall.realtime.common.constant.Constant;

/**
 * ClassName: SQLUtil
 * Package: com.matthew.gmall.realtime.common.util
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/15 9:34
 * @Version 1.0
 */
public class SQLUtil {
    public static String getKafkaDDLSource(String topic,String groupId){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topic + "',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '"+ groupId +"',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                // 当 json 解析失败的时候,忽略这条数据
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaDDLSink(String topic){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topic + "',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic){
        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.ignore-parse-errors' = 'true'," +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";

    }
}
