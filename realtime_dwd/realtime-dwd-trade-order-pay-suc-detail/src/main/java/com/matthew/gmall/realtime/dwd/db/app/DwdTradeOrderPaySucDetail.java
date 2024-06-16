package com.matthew.gmall.realtime.dwd.db.app;

import com.matthew.gmall.realtime.common.base.BaseSQLAPP;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeOrderPaySucDetail
 * Package: com.matthew.gmall.realtime.dwd.db.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/16 15:56
 * @Version 1.0
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //1.读取topic_db的数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        //2.过滤支付成功的数据
        Table paySuc = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['user_id'] as user_id,\n" +
                        "\tafter['order_id'] as order_id,\n" +
                        "\tafter['payment_type'] as payment_type,\n" +
                        "\tafter['callback_time'] as callback_time,\n" +
                        "\tts_ms as ts,\n" +
                        "\tpt,\n" +
                        "\tet\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'payment_info'\n" +
                        "and op = 'u' and before['payment_status'] is not null and after['payment_status'] = '1602'"
        );
        tableEnv.createTemporaryView("pay_suc_info",paySuc);

        //3.读取下单明细数据表
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
                        "\tet as TO_TIMESTAMP_LTZ(ts,3),\n" +
                        "\tWATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                        ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        //4.读取字典表
        readBaseDic(tableEnv);

        //5.下单明细数据表和支付成功明细表 interval join，再和字典表look up join
        Table resultTable = tableEnv.sqlQuery(
                "select \n" +
                        "\tt2.id order_detail_id, \n" +
                        "\tt2.order_id, \n" +
                        "\tt2.user_id, \n" +
                        "\tt2.sku_id, \n" +
                        "\tt2.sku_name, \n" +
                        "\tt2.province_id, \n" +
                        "\tt2.activity_id, \n" +
                        "\tt2.activity_rule_id, \n" +
                        "\tt2.coupon_id, \n" +
                        "\tt1.payment_type payment_type_code , \n" +
                        "\tt3.dic_name payment_type_name, \n" +
                        "\tt1.callback_time, \n" +
                        "\tt2.sku_num, \n" +
                        "\tt2.split_original_amount, \n" +
                        "\tt2.split_activity_amount, \n" +
                        "\tt2.split_coupon_amount, \n" +
                        "\tt2.split_total_amount split_payment_amount, \n" +
                        "\tt1.ts  \n" +
                        "from pay_suc_info t1 \n" +
                        "join dwd_trade_order_detail t2 \n" +
                        "on t1.order_id = t2.order_id\n" +
                        "AND t2.et BETWEEN t1.et - INTERVAL '15' MINUTE AND t1.et + INTERVAL '5' SECOND\n" +
                        "left join base_dic FOR SYSTEM_TIME AS OF t1.pt AS t3\n" +
                        "on t1.payment_type = t3.dic_code"
        );

        //6.建kafka映射表
        tableEnv.executeSql("create table "+ Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS +"(" +
                        "order_detail_id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "payment_type_code string," +
                        "payment_type_name string," +
                        "callback_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_payment_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS)
        );

        //7.结果写入kafka中
        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();



    }
}
