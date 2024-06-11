package com.matthew.gmall.realtime.common.util;

import com.matthew.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.table.catalog.ObjectPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

/**
 * ClassName: FlinkSourceUtil
 * Package: com.matthew.gmall.realtime.common.util
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/28 22:13
 * @Version 1.0
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                //处理序列器不能处理空消息的bug
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }

    //mysql-CDC-Source
    public static MySqlSource<String> getMysqlCDCSource(String serverId,String dataBaseList,String tableList,StartupOptions startupOptions){
        //debeziumProperties 的设置
        Properties properties = new Properties();
        properties.setProperty("converters","dateConverters");
        properties.setProperty("dateConverters.type","com.matthew.gmall.realtime.common.function.MySqlDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");

        //Jdbc Properties 的设置
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        //ods的MySQLSource
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .serverId(serverId)
                .jdbcProperties(props)
                .databaseList(dataBaseList)
                .tableList(tableList)
                //无主键表指定一列为分片列
                .chunkKeyColumn(new ObjectPath("gamll","id"),"id")
                .startupOptions(startupOptions)
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .connectTimeout(Duration.ofMinutes(1L))
                .build();
    }

}
