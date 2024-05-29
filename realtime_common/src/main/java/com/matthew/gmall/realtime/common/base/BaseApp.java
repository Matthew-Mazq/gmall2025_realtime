package com.matthew.gmall.realtime.common.base;

import com.matthew.gmall.realtime.common.util.CreateEnvUtil;
import com.matthew.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: BaseApp
 * Package: com.matthew.gmall.realtime.common.base
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/28 22:16
 * @Version 1.0
 */
public abstract class BaseApp {
    public abstract void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> stream);

    public void start(int port,int parallelism,String ckAndGroupId,String topic){
        //todo 1.获取流执行环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(port, parallelism, ckAndGroupId);

        //todo 3.从kafka source读取数据，封装为流
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);

        SingleOutputStreamOperator<String> strDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid(ckAndGroupId + "kafka_source");

        //todo 4.具体的处理逻辑
        handle(env,strDS);

        //todo 5.执行job
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
