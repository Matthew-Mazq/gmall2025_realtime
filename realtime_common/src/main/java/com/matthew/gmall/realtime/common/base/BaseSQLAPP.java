package com.matthew.gmall.realtime.common.base;

import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.CreateEnvUtil;
import com.matthew.gmall.realtime.common.util.FlinkSourceUtil;
import com.matthew.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: BaseSQLAPP
 * Package: com.matthew.gmall.realtime.common.base
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/15 9:46
 * @Version 1.0
 */
public abstract class BaseSQLAPP {
    public void start(int port,int parallelism,String ckAndGroupId){
        System.setProperty("HADOOP_USER_NAME","root");
        //todo 1.获取流执行环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(port, parallelism, ckAndGroupId);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        handle(tableEnv,env);

    }

    public abstract void handle(StreamTableEnvironment tableEnv,StreamExecutionEnvironment env);

    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId){
        tableEnv.executeSql(
                "create table topic_db(\n" +
                        " before map<string,string>,\n" +
                        " after map<string,string>,\n" +
                        " source map<string,string>,\n" +
                        " op string,\n" +
                        " ts_ms bigint,\n" +
                        "  `pt` as proctime(), " +
                        "  et as to_timestamp_ltz(ts_ms, 3), " +
                        "  watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DB,groupId)
        );
    }

    public void readBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql(
                "CREATE TABLE base_dic (\n" +
                        " dic_code string,\n" +
                        " info ROW<dic_name string>,\n" +
                        " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'hbase-2.2',\n" +
                        " 'table-name' = 'gmall:dim_base_dic',\n" +
                        " 'zookeeper.quorum' = 'node1:2181,node2:2181,node3:2181',\n" +
                        " 'lookup.cache' = 'PARTIAL', " +
                        " 'lookup.async' = 'true', " +
                        " 'lookup.partial-cache.max-rows' = '20', " +
                        " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                        ")"
        );
    }
}
