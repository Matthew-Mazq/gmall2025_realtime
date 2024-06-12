package com.matthew.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.DateFormatUtil;
import com.matthew.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

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
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> strDS){
        //1.进行数据的etl，过滤非标准的数据
        SingleOutputStreamOperator<JSONObject> filterDS = filerEtl(strDS);
        //2.新老用户的修复
        SingleOutputStreamOperator<JSONObject> isNewFixDS = isNewFix(filterDS);
        // isNewFixDS.printToErr();



        //3.分流
        Map<String, DataStream<JSONObject>> splitDS = splitStream(isNewFixDS);

        //4.写入kafka
        writeToKafka(splitDS);
    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> streams){
        streams.get(START)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        streams.get(ERR)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        streams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

        streams
                .get(ACTION)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));


    }
    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream){
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err"){};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};
        /*
        主流: 启动日志
        侧输出流: 页面 错误 曝光 活动
         */
        SingleOutputStreamOperator<JSONObject> startStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        JSONObject common = obj.getJSONObject("common");
                        Long ts = obj.getLong("ts");
                        // 1. 启动
                        JSONObject start = obj.getJSONObject("start");
                        if (start != null) {
                            out.collect(obj);
                        }

                        // 2. 曝光
                        JSONArray displays = obj.getJSONArray("displays");
                        if (displays != null) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.putAll(common);
                                display.put("ts", ts);
                                ctx.output(displayTag, display);
                            }

                            // 删除displays
                            obj.remove("displays");
                        }
                        // 3. 活动
                        JSONArray actions = obj.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.putAll(common);
                                ctx.output(actionTag, action);
                            }

                            // 删除displays
                            obj.remove("actions");
                        }

                        // 4. err
                        JSONObject err = obj.getJSONObject("err");
                        if (err != null) {
                            ctx.output(errTag, obj);
                            obj.remove("err");
                        }

                        // 5. 页面
                        JSONObject page = obj.getJSONObject("page");
                        if (page != null) {
                            ctx.output(pageTag, obj);
                        }

                    }
                });

        Map<String, DataStream<JSONObject>> streams = new HashMap<>();
        startStream.print("start");
        SideOutputDataStream<JSONObject> displayDS = startStream.getSideOutput(displayTag);
        displayDS.print("display");
        SideOutputDataStream<JSONObject> errDS = startStream.getSideOutput(errTag);
        errDS.print("err");
        SideOutputDataStream<JSONObject> pageDS = startStream.getSideOutput(pageTag);
        pageDS.print("page");
        SideOutputDataStream<JSONObject> actionDS = startStream.getSideOutput(actionTag);
        actionDS.print("action");

        streams.put(START, startStream);
        streams.put(DISPLAY, displayDS);
        streams.put(ERR, errDS);
        streams.put(PAGE, pageDS);
        streams.put(ACTION, actionDS);

        return streams;

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
