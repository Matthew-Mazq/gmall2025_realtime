package com.matthew.gmall.realtime.dwd.db.app.com.matthewgmall.realtime.dwd.db.app;

import com.matthew.gmall.realtime.common.base.BaseSQLAPP;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderDetail
 * Package: com.matthew.gmall.realtime.dwd.db.app.com.matthewgmall.realtime.dwd.db.app
 * Description:  交易域下单事务事实表
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/15 17:21
 * @Version 1.0
 */
public class DwdTradeOrderDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //设置状态的超时时间-5秒
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        //1.读取topic_db
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        //2.过滤订单明细数据
        Table orderDetail = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['id'] as id,\n" +
                        "\tafter['order_id'] as order_id,\n" +
                        "\tafter['sku_id'] as sku_id,\n" +
                        "\tafter['sku_name'] as sku_name,\n" +
                        "\tafter['order_price'] as order_price,\n" +
                        "\tafter['sku_num'] as sku_num,\n" +
                        "\tafter['create_time'] as create_time,\n" +
                        "\tcast(cast(after['sku_num'] as decimal(16,2)) * cast(after['order_price'] as decimal(16,2))\n" +
                        "\tas string) as split_original_amount,\n" +
                        "\tafter['split_total_amount'] split_total_amount,\n" +
                        "\tafter['split_activity_amount'] split_activity_amount,\n" +
                        "\tafter['split_coupon_amount'] split_coupon_amount,\n" +
                        "\tts_ms as ts\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'order_detail'\n" +
                        "and op in ('c','r','u')"
        );
        tableEnv.createTemporaryView("order_detail",orderDetail);

        //3.过滤订单order_info数据，关联join订单明细
        Table orderInfo = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['id'] as id,\n" +
                        "\tafter['user_id'] as user_id,\n" +
                        "\tafter['province_id'] as province_id\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'order_info'\n" +
                        "and op in ('c','r','u')"
        );

        tableEnv.createTemporaryView("order_info",orderInfo);

        //4.读取订单活动表
        Table orderActivity = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['order_detail_id'] as order_detail_id,\n" +
                        "\tafter['activity_id'] as activity_id,\n" +
                        "\tafter['activity_rule_id'] as activity_rule_id\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'order_detail_activity'\n" +
                        "and op in ('c','r','u')"
        );

        tableEnv.createTemporaryView("order_detail_activity",orderActivity);

        //5.读取订单优惠券表
        Table orderCoupon = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['order_detail_id'] as order_detail_id,\n" +
                        "\tafter['coupon_id'] as coupon_id\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'order_detail_coupon'\n" +
                        "and op in ('c','r','u')"
        );

        tableEnv.createTemporaryView("order_detail_coupon",orderCoupon);

        //关联订单明细表和订单信息表(join)、和订单活动、订单优惠券(left join)
        Table resultTable = tableEnv.sqlQuery(
                "select \n" +
                        "\tt1.id,\n" +
                        "\tt1.order_id,\n" +
                        "\tt2.user_id,\n" +
                        "\tt2.province_id,\n" +
                        "\tt3.activity_id,\n" +
                        "\tt3.activity_rule_id,\n" +
                        "\tt4.coupon_id,\n" +
                        "\tt1.sku_id,\n" +
                        "\tt1.sku_name,\n" +
                        "\tt1.order_price,\n" +
                        "\tt1.sku_num,\n" +
                        "\tt1.create_time,\n" +
                        "\tt1.split_original_amount,\n" +
                        "\tt1.split_total_amount,\n" +
                        "\tt1.split_activity_amount,\n" +
                        "\tt1.split_coupon_amount,\n" +
                        "\tt1.ts\n" +
                        "from order_detail t1 \n" +
                        "join order_info t2 \n" +
                        "on t1.order_id = t2.id\n" +
                        "left join order_detail_activity t3 \n" +
                        "on t3.order_detail_id = t1.id\n" +
                        "left join order_detail_coupon t4 \n" +
                        "on t4.order_detail_id = t1.id"
        );

        //建立kafka的映射表
        tableEnv.executeSql(
                "create table "+ Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +"(\n" +
                        "\tid string,\n" +
                        "\torder_id string,\n" +
                        "\tuser_id string,\n" +
                        "\tprovince_id string,\n" +
                        "\tactivity_id string,\n" +
                        "\tactivity_rule_id string,\n" +
                        "\tcoupon_id string,\n" +
                        "\tsku_id string,\n" +
                        "\tsku_name string,\n" +
                        "\torder_price string,\n" +
                        "\tsku_num string,\n" +
                        "\tcreate_time string,\n" +
                        "\tsplit_original_amount string,\n" +
                        "\tsplit_total_amount string,\n" +
                        "\tsplit_activity_amount string,\n" +
                        "\tsplit_coupon_amount string,\n" +
                        "\tts bigint,\n" +
                        "\tPRIMARY KEY (id) NOT ENFORCED\n" +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        //把数据写入到kafka对应主题中
        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();


    }
}
