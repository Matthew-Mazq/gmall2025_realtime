package com.matthew.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.base.BaseApp;
import com.matthew.gmall.realtime.common.bean.TrafficPageViewBean;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: DwsTrafficVcChArIsNewPageViewWindow
 * Package: com.matthew.gmall.realtime.dws.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/26 22:22
 * @Version 1.0
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10021,4,"dws_vc_ch_isNew_pageView", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, SingleOutputStreamOperator<String> strDS) {
        //1.解析数据，封装到bean中
        SingleOutputStreamOperator<TrafficPageViewBean> withBeanDS = toTrafficBean(strDS);

        //2.开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> windowAggDS = withBeanDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                            @Override
                                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                                .withIdleness(Duration.ofSeconds(120L))
                ).keyBy(bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew())
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                                //有且只有一个值，前面聚合的最终结果
                                TrafficPageViewBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                                bean.setCur_date(DateFormatUtil.tsToDateForPartition(context.window().getStart()));
                                out.collect(bean);
                            }
                        }
                );
        windowAggDS.printToErr();


    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> toTrafficBean(SingleOutputStreamOperator<String> strDS) {
        return strDS.map(str -> JSON.parseObject(str))
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                            private ValueState<String> lastVisitDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                lastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVisitDateState", String.class));
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                                JSONObject page = jsonObj.getJSONObject("page");
                                JSONObject common = jsonObj.getJSONObject("common");
                                Long ts = jsonObj.getLong("ts");
                                String today = DateFormatUtil.tsToDate(ts);
                                Long pv = 1L;
                                Long durSum = page.getLong("during_time");
                                String lastVisitDate = lastVisitDateState.value();
                                Long uv = 0L;
                                if (!today.equals(lastVisitDate)) {
                                    //是当天的第一次访问
                                    uv = 1L;
                                    //更新状态
                                    lastVisitDateState.update(today);
                                }
                                TrafficPageViewBean bean = new TrafficPageViewBean();
                                bean.setVc(common.getString("vc"));
                                bean.setCh(common.getString("ch"));
                                bean.setAr(common.getString("ar"));
                                bean.setIsNew(common.getString("is_new"));
                                bean.setPvCt(pv);
                                bean.setUvCt(uv);
                                bean.setDurSum(durSum);
                                bean.setTs(ts);
                                bean.setSid(common.getString("sid"));
                                out.collect(bean);
                            }
                        }
                )
                //按照sid分组，计算会话数sv使用
                .keyBy(TrafficPageViewBean::getSid)
                .process(
                        new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {
                            private ValueState<Boolean> isFirstState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<Boolean> stateDesc = new ValueStateDescriptor<>("isFirst", Boolean.class);
                                //状态的生命周期
                                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(org.apache.flink.api.common.time.Time.hours(1L))
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .useProcessingTime()
                                        .build();
                                stateDesc.enableTimeToLive(stateTtlConfig);
                                isFirstState = getRuntimeContext().getState(stateDesc);
                            }

                            @Override
                            public void processElement(TrafficPageViewBean bean, Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                                if(isFirstState.value() == null){
                                    bean.setSvCt(1L);
                                    isFirstState.update(true);
                                }else{
                                    isFirstState.update(false);
                                }
                                out.collect(bean);
                            }
                        }
                );
    }
}
