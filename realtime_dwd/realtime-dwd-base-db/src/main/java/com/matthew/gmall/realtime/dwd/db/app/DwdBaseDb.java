package com.matthew.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.bean.TableProcessDwd;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.FlinkSinkUtil;
import com.matthew.gmall.realtime.common.util.FlinkSourceUtil;
import com.matthew.gmall.realtime.common.util.JdbcUtil;
import com.sun.xml.internal.fastinfoset.util.ValueArray;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.matthew.gmall.realtime.dwd.db.app.DwdBaseDb.filterColumns;

/**
 * ClassName: DwdBaseDb
 * Package: com.matthew.gmall.realtime.dwd.db.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/16 18:17
 * @Version 1.0
 */
@Slf4j
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10017,4, "dwd_base_db",Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> strDS) {
        //1.过滤脏数据，非json格式的数据
        SingleOutputStreamOperator<String> afterFilterDS = strDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            if(!"d".equals(jsonObj.getString("op"))){
                                out.collect(jsonStr);
                            }

                        } catch (Exception e) {
                            log.error("不是标准的json格式，已被过滤掉.....");
                        }
                    }
                }
        );

        //2.读取配置表的数据
        SingleOutputStreamOperator<TableProcessDwd> configDwdDS = getConfigDwdDS(env);
        //TableProcessDwd(sourceTable=user_info, sourceType=c, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r)

        //3.广播配置表的数据
        MapStateDescriptor<String, TableProcessDwd> dwdMapStateDescriptor =
                new MapStateDescriptor<>("config_dwd", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadDS = configDwdDS.broadcast(dwdMapStateDescriptor);

        //4.关联主流和配置流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connectDS = afterFilterDS.connect(broadDS).process(
                new BroadcastProcessFunction<String, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    private HashMap<String, TableProcessDwd> configMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //预加载配置信息
                        Connection conn = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> processDwdList = JdbcUtil.queryList(conn, "select * from " + Constant.CONFIG_DWD_TABLE_LIST,
                                TableProcessDwd.class, true);
                        for (TableProcessDwd processDwd : processDwdList) {
                            String key = processDwd.getSourceTable() + ":" + processDwd.getSourceType();
                            configMap.put(key, processDwd);
                        }

                        JdbcUtil.closeConnection(conn);
                    }
                    @Override
                    public void processBroadcastElement(TableProcessDwd processDwd, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        String op = processDwd.getOp();
                        String key = processDwd.getSourceTable() + ":" + processDwd.getSourceType();
                        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(dwdMapStateDescriptor);
                        if ("d".equals(op)) {
                            //从状态和配置中移除
                            broadcastState.remove(key);
                            configMap.remove(key);
                        } else {
                            broadcastState.put(key, processDwd);
                            configMap.put(key, processDwd);
                        }
                    }

                    @Override
                    public void processElement(String jsonStr, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(dwdMapStateDescriptor);
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject afterJsonObj = jsonObj.getJSONObject("after");
                        String op = jsonObj.getString("op");
                        String sourceTable = jsonObj.getJSONObject("source").getString("table");
                        String key = sourceTable + ":" + op;
                        TableProcessDwd processDwd = broadcastState.get(key);
                        if (processDwd == null) {
                            processDwd = configMap.get(key);
                        }

                        if (processDwd != null) {
                            //说明是配置表中的数据，需要放入到主流中，根据配置的信息过滤出只需要的字段
                            filterColumns(afterJsonObj, processDwd);
                            out.collect(Tuple2.of(afterJsonObj, processDwd));
                        }
                    }
                }
        );
        // connectDS.printToErr();
        connectDS.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopic());

    }




    public static void filterColumns(JSONObject afterJsonObj, TableProcessDwd processDwd) {
        List<String> columnList = new ArrayList<>(Arrays.asList(processDwd.getSinkColumns().split(",")));
        afterJsonObj.keySet().removeIf(key -> !columnList.contains(key));
    }

    public static SingleOutputStreamOperator<TableProcessDwd> getConfigDwdDS(StreamExecutionEnvironment env) {

        MySqlSource<String> configDwdSource = FlinkSourceUtil.getMysqlCDCSource("5800", Constant.CONFIG_DWD_DATABASE,
                Constant.CONFIG_DWD_TABLE_LIST, StartupOptions.initial());

        return env.fromSource(configDwdSource, WatermarkStrategy.noWatermarks(), "config_dwd_source")
                .uid("config_dwd_source").setParallelism(1)
                .map(
                        new MapFunction<String, TableProcessDwd>() {
                            @Override
                            public TableProcessDwd map(String jsonStr) throws Exception {
                                JSONObject jsonObj = JSON.parseObject(jsonStr);
                                String op = jsonObj.getString("op");
                                JSONObject beforeJson = jsonObj.getJSONObject("before");
                                JSONObject afterJson = jsonObj.getJSONObject("after");
                                TableProcessDwd processDwd;
                                if ("d".equals(op)) {
                                    processDwd = jsonObj.getObject("before",TableProcessDwd.class);
                                }else{
                                    processDwd = jsonObj.getObject("after", TableProcessDwd.class);
                                }
                                processDwd.setOp(op);
                                return processDwd;
                            }
                        }
                )
                .setParallelism(1);
    }
}
