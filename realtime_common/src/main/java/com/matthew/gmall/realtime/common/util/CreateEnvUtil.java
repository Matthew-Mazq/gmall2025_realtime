package com.matthew.gmall.realtime.common.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: CreateEnvUtil
 * Package: com.matthew.gmall.realtime.common.util
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/28 23:18
 * @Version 1.0
 */
public class CreateEnvUtil {
    public static StreamExecutionEnvironment getStreamEnv(int port,int parallelism,String ckAndGroupId){
        //todo 1.设置操作HDFS的用户
        System.setProperty("HADOOP_USER_NAME","root");
        //todo 2.获取和设置流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        //设定重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1),Time.seconds(3)));

        //开启检查点
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig chkConfig = env.getCheckpointConfig();
        chkConfig.setMinPauseBetweenCheckpoints(30000L);
        chkConfig.setCheckpointTimeout(120000L);
        chkConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());
        chkConfig.setCheckpointStorage("hdfs://node1:8020//flink-dirs/chk/" + ckAndGroupId);

        return env;
    }
}
