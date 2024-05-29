package com.matthew.gmall.realtime.common.util;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

/**
 * ClassName: KafkaUtil
 * Package: com.matthew.gmall.realtime.common.util
 * Description:封装操作kafka的工具类
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/25 17:46
 * @Version 1.0
 */
public class KafkaUtil {
    public static final String KAFKA_SERVER = "node1:9020,node2:9020,node3:9020";



}
