package com.matthew.ods;


import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.CreateEnvUtil;
import com.matthew.gmall.realtime.common.util.FlinkSinkUtil;
import com.matthew.gmall.realtime.common.util.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: OdsApp
 * Package: com.matthew.gmall.realtime.common.ods
 * Description: 同步数据到kafka中
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/27 21:13
 * @Version 1.0
 */
public class OdsApp{
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(19999, 1, "ods_app");
        //todo 1.读取业务库的数据，封装为流
        MySqlSource<String> mysqlCDCSource = FlinkSourceUtil.getMysqlCDCSource("5405", "gmall", Constant.DIM_TABLE_LIST, StartupOptions.initial());
        SingleOutputStreamOperator<String> odsDimSource = env.fromSource(mysqlCDCSource, WatermarkStrategy.noWatermarks(), "ods_dim_source")
                .uid("ods_dim_source");
        //todo 2.获取kafka-sink
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DB);

        //todo 3.将数据写入到kafka中
        odsDimSource.sinkTo(kafkaSink);

        env.execute();
    }
}
