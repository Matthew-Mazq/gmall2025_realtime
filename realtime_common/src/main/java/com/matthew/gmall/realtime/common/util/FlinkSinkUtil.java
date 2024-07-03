package com.matthew.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.bean.TableProcessDwd;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

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

    public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSinkWithTopic(){
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> dataWithConfig, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String topic = dataWithConfig.f1.getSinkTable();
                                JSONObject data = dataWithConfig.f0;
                                data.remove("op");
                                return new ProducerRecord<>(topic,data.toJSONString().getBytes(StandardCharsets.UTF_8));
                            }
                        }
                )
                // Flink重启启动的时候，如果我的应用程序包含一个Sink to Kafka（EXACTLY_ONCE交付）所以关闭精准一次性，不优雅的关闭会有很多事务
                // 程序处于"卡死"状态，后面写到kafka的消息也是uncommitted的
                // .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // .setTransactionalIdPrefix(transIdPrefix)
                // .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,transactionTimeout)
                .build();
    }

    public static SinkFunction<String> getSRSink(String tableName, String labelPre){
            StarRocksSinkOptions options = StarRocksSinkOptions.<String>builder()
                    .withProperty("jdbc-url", "jdbc:mysql://"+ Constant.STARROCKS_FE_IP +":9030")
                    .withProperty("load-url", Constant.STARROCKS_FE_IP +":8030'")
                    .withProperty("database-name", "gmall2023_realtime")
                    .withProperty("table-name", tableName)
                    .withProperty("username", "root")
                    .withProperty("password", "123456")
                    .withProperty("sink.properties.format", "json")
                    .withProperty("sink.label-prefix",labelPre + System.currentTimeMillis())
                    .withProperty("sink.properties.strip_outer_array", "true")
                    .build();
            // Create the sink with the options.
        return StarRocksSink.sink(options);

    }
}
