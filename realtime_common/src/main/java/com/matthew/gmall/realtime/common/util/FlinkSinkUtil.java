package com.matthew.gmall.realtime.common.util;

import com.matthew.gmall.realtime.common.constant.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * ClassName: FlinkSinkUtil
 * Package: com.matthew.gmall.realtime.common.util
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/28 23:15
 * @Version 1.0
 */
public class FlinkSinkUtil {
    //获取kafka sink
    public static KafkaSink<String> getKafkaSink(String topic){
        if(topic == null){
            throw new RuntimeException("topic为空，请确认kafkaSink!");
        }
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // Flink重启启动的时候，如果我的应用程序包含一个Sink to Kafka（EXACTLY_ONCE交付）所以关闭精准一次性，不优雅的关闭会有很多事务
                // 程序处于"卡死"状态，后面写到kafka的消息也是uncommitted的
                // .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // .setTransactionalIdPrefix(transIdPrefix)
                // .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,transactionTimeout)
                .build();
    }
}
