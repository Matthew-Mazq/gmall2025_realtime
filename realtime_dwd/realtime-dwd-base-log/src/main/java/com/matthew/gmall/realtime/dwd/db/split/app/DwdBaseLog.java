package com.matthew.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.DateFormatUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: DwdBaseLog
 * Package: com.matthew.gmall.realtime.dwd.db.split.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/11 22:14
 * @Version 1.0
 */
@Slf4j
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> strDS){
        strDS.printToErr();
        //1.进行数据的etl，过滤非标准的数据
        SingleOutputStreamOperator<JSONObject> filterDS = filerEtl(strDS);
        //2.新老用户的修复
        SingleOutputStreamOperator<JSONObject> isNewFixDS = isNewFix(filterDS);
        // isNewFixDS.printToErr();

    }

    private static SingleOutputStreamOperator<JSONObject> isNewFix(SingleOutputStreamOperator<JSONObject> streamDS){
        return streamDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            private ValueState<String> firstVisitDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateState", String.class));
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject common = jsonObj.getJSONObject("common");
                                //获取是否新用户
                                String isNew = common.getString("is_new");

                                Long ts = jsonObj.getLong("ts");
                                String today = DateFormatUtil.tsToDate(ts);
                                //从状态中获取首次访问的日期
                                String firstVisitDate = firstVisitDateState.value();

                                if ("1".equals(isNew)) {
                                    if (firstVisitDate == null) {
                                        //更新当前日期到状态中
                                        firstVisitDateState.update(today);
                                    } else if (!today.equals(firstVisitDate)) {
                                        //状态不为空，且首次访问日期不是今天
                                        common.put("is_new", "0");
                                    }
                                } else {
                                    //is_new = 0,老用户
                                    if (firstVisitDate == null) {
                                        //是老用户，但是状态里的日期为空，则把状态补充为昨天
                                        firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                                    }
                                }
                                out.collect(jsonObj);
                            }
                        }
                );
    }

    private static SingleOutputStreamOperator<JSONObject> filerEtl(SingleOutputStreamOperator<String> strDS) {
        return strDS.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            log.error("日志格式不是正确的Json格式:" + jsonStr);
                        }
                    }
                }
        );
    }
}
