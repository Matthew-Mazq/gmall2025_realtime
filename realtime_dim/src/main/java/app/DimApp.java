package app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.bean.TableProcessDim;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.FlinkSourceUtil;
import com.matthew.gmall.realtime.common.util.*;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: DimApp
 * Package: app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/29 22:10
 * @Version 1.0
 */
@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> strDS) {
        //todo 1.进行数据过滤
        SingleOutputStreamOperator<JSONObject> jsonObjDS = strDS.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            log.warn("不是标准的JSON格式数据!!");
                        }

                    }
                }
        );

        //todo 2.flinkCDC读取配置表的数据
        MySqlSource<String> configSource = FlinkSourceUtil.getMysqlCDCSource("5409", "gmall2023_config", "gmall2023_config.table_process_dim"
                , StartupOptions.initial());
        SingleOutputStreamOperator<String> configDS = env.fromSource(configSource, WatermarkStrategy.noWatermarks(), "dim_config_source")
                .uid("dim_config_source").setParallelism(1);

        //todo 3.将配置表的数据转为javaBean,同时在hbase中创建表
        /**
         * 配置流的并行度必须为1，否则在配置信息变更时会出现乱序，进而导致一致性问题。比如某条配置信息由A变为B，再由B变为C，
         * 当配置流并行度不为1，且第一次变更和第二次变更日志进入不同分区时，第二次变更可能先到达，下游获取的配置信息会先由A变为C，再由C变为B，与上游不一致。
         */
        SingleOutputStreamOperator<TableProcessDim> configMapDS = configDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        TableProcessDim dim;
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取操作的数据类型
                        String op = jsonObj.getString("op");
                        //判断操作的类型决定取before还是after的数据
                        if ("d".equals(op)) {
                            dim = jsonObj.getObject("before", TableProcessDim.class);
                            HBaseUtil.dropHbaseTable(Constant.HBASE_NAMESPACE,dim.getSinkTable());
                        } else if ("r".equals(op) || "c".equals(op)){
                            dim = jsonObj.getObject("after", TableProcessDim.class);

                            HBaseUtil.createHbaseTable(Constant.HBASE_NAMESPACE, dim.getSinkTable()
                                    ,dim.getSinkFamily().split(","));
                        }else{
                            //更新操作，要先删除表再创建表
                            dim = jsonObj.getObject("after", TableProcessDim.class);
                            HBaseUtil.dropHbaseTable(Constant.HBASE_NAMESPACE,dim.getSinkTable());
                            HBaseUtil.createHbaseTable(Constant.HBASE_NAMESPACE, dim.getSinkTable()
                                    ,dim.getSinkFamily().split(","));
                        }
                        dim.setOp(op);
                        return dim;
                    }
                }
        ).setParallelism(1);

        //TableProcessDim(sourceTable=base_dic, sinkTable=dim_base_dic, sinkColumns=dic_code,dic_name, sinkFamily=info, sinkRowKey=dic_code, op=r)
        configMapDS.printToErr();

    }
}
