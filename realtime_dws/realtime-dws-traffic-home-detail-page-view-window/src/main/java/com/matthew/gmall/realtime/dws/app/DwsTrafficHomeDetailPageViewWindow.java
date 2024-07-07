package com.matthew.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.DateFormatUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: DwsTrafficHomeDetailPageViewWindow
 * Package: com.matthew.gmall.realtime.dws.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/7/5 21:47
 * @Version 1.0
 */
@Slf4j
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10025,4,"DwsTrafficHomeDetailPageViewWindow", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> strDS) {
        //处理的主逻辑

        //1.转换数据结构，并过滤掉数据page_id in (home,good_detail),mid分组，不能为空
        SingleOutputStreamOperator<JSONObject> flatMapDS = filterDs(strDS);

        //2.设置水位线
        KeyedStream<JSONObject, String> withWaterDS = setWM(flatMapDS);

        //4.计算首页和商品详情页的独立访客数
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processDS = caclUvCt(withWaterDS);

        //5.开窗，聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                long windowStart = ctx.window().getStart();
                                String stt = DateFormatUtil.tsToDateTime(windowStart);

                                long windowEnd = ctx.window().getEnd();
                                String edt = DateFormatUtil.tsToDateTime(windowEnd);
                                for (TrafficHomeDetailPageViewBean bean : elements) {
                                    //迭代器里面经过聚合以后实际上只有一条数据
                                    bean.setStt(stt);
                                    bean.setEdt(edt);
                                }
                            }
                        }
                );



    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> caclUvCt(KeyedStream<JSONObject, String> withWaterDS) {
         return withWaterDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    //存放首页访问时间的状态
                    private ValueState<String> homeLastVisitState;
                    private ValueState<String> detailLastVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homePageStateDesc = new ValueStateDescriptor<>("homePageVisitState", String.class);
                        StateTtlConfig homeConfig = new StateTtlConfig.Builder(Time.days(1L)).build();
                        //设置状态的生命周期1天
                        homePageStateDesc.enableTimeToLive(homeConfig);
                        homeLastVisitState = getRuntimeContext().getState(homePageStateDesc);

                        ValueStateDescriptor<String> detailStateDesc = new ValueStateDescriptor<>("detailPageVisitState", String.class);
                        StateTtlConfig detailConfig = new StateTtlConfig.Builder(Time.days(1L)).build();
                        detailStateDesc.enableTimeToLive(detailConfig);
                        detailLastVisitState = getRuntimeContext().getState(detailStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        //区分首页和详情页分别计算
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String curDate = DateFormatUtil.tsToDate(ts);
                        Long homeUvCt = 0L;
                        Long goodDetailUvCt = 0L;
                        //首页
                        if ("home".equals(pageId)) {
                            String homeLastVisit = homeLastVisitState.value();
                            //状态为空或状态的日期不等于今日
                            if (homeLastVisit == null || !curDate.equals(homeLastVisit)) {
                                homeUvCt = 1L;
                                homeLastVisitState.update(curDate);
                            }
                        } else {
                            //商品详情页
                            String detailLastVisit = detailLastVisitState.value();
                            //状态为空或状态的日期不等于今日
                            if (detailLastVisit == null || !curDate.equals(detailLastVisit)) {
                                goodDetailUvCt = 1L;
                                detailLastVisitState.update(curDate);
                            }
                        }

                        //封装bean,向下游传输,传输之前判断homeUvCt或goodDetailUvCt至少有一个不为0
                        if(homeUvCt != 0 || goodDetailUvCt != 0){
                            TrafficHomeDetailPageViewBean bean = TrafficHomeDetailPageViewBean.builder()
                                    .curDate(curDate)
                                    .homeUvCt(homeUvCt)
                                    .goodDetailUvCt(goodDetailUvCt)
                                    .ts(ts)
                                    .build();
                            out.collect(bean);
                        }
                    }
                }
        );
    }

    private static KeyedStream<JSONObject, String> setWM(SingleOutputStreamOperator<JSONObject> flatMapDS) {
        return flatMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner((jsonObj, record) -> jsonObj.getLong("ts"))
                        //空闲等待时间
                        .withIdleness(Duration.ofSeconds(5L))
        ).keyBy(
                //3.分组
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
    }

    private static SingleOutputStreamOperator<JSONObject> filterDs(SingleOutputStreamOperator<String> strDS) {
        return strDS.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String pageId = jsonObj.getJSONObject("page").getString("page_id");
                            String mid = jsonObj.getJSONObject("common").getString("mid");
                            if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                                if (mid != null) {
                                    out.collect(jsonObj);
                                }
                            }
                        } catch (Exception e) {
                            log.error("非标准JSON，脏数据被过滤掉");
                        }
                    }
                }
        );
    }
}
