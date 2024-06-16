package com.matthew.gmall.realtime.dwd.db.app;

import com.matthew.gmall.realtime.common.base.BaseSQLAPP;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.FlinkSinkUtil;
import com.matthew.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderRefund
 * Package: com.matthew.gmall.realtime.dwd.db.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/16 17:09
 * @Version 1.0
 */
public class DwdTradeOrderRefund extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10016,4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        //1.读取topic_db的数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

        //2.过滤订单信息表中的退单数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['id'] as id,\n" +
                        "\tafter['user_id'] as user_id,\n" +
                        "\tafter['province_id'] as province_id\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'order_info'\n" +
                        "and op = 'u' and before['order_status'] is not null and after['order_status'] = '1005'"
        );

        tableEnv.createTemporaryView("order_refund_info",orderRefundInfo);

        //3.过滤退单明细表数据
        Table refundDetail = tableEnv.sqlQuery(
                "select \n" +
                        "\tafter['id'] as id,\n" +
                        "\tafter['user_id'] as user_id,\n" +
                        "\tafter['order_id'] as order_id,\n" +
                        "\tafter['sku_id'] as sku_id,\n" +
                        "\tafter['refund_type'] as refund_type,\n" +
                        "\tafter['refund_num'] as refund_num,\n" +
                        "\tafter['refund_amount'] as refund_amount,\n" +
                        "\tafter['refund_reason_type'] as refund_reason_type,\n" +
                        "\tafter['refund_reason_txt'] as refund_reason_txt,\n" +
                        "\tafter['refund_status'] as refund_status,\n" +
                        "\tts_ms as ts,\n" +
                        "\tpt\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'order_refund_info'\n" +
                        "and op in ('u','c','r')"
        );
        tableEnv.createTemporaryView("refund_detail",refundDetail);

        //4.读取字典表
        readBaseDic(tableEnv);


        //5.关联4张表
        Table resultTable = tableEnv.sqlQuery(
                "select \n" +
                        "\tt1.id,\n" +
                        "\tt1.user_id,\n" +
                        "\tt2.province_id,\n" +
                        "\tt1.order_id,\n" +
                        "\tt1.sku_id,\n" +
                        "\tt1.refund_type as refund_type_code,\n" +
                        "\tt3.info.dic_name as refund_type_name,\n" +
                        "\tt1.refund_num,\n" +
                        "\tt1.refund_amount,\n" +
                        "\tt1.refund_reason_type as refund_reason_type_code,\n" +
                        "\tt4.info.dic_name as refund_reason_type_name,\n" +
                        "\tt1.refund_reason_txt,\n" +
                        "\tt1.refund_status,\n" +
                        "\tt1.ts\n" +
                        "from refund_detail t1 \n" +
                        "join order_refund_info t2 \n" +
                        "on t1.order_id = t2.id\n" +
                        "left join base_dic FOR SYSTEM_TIME AS OF t1.pt AS t3 \n" +
                        "on t1.refund_type = t3.dic_code\n" +
                        "left join base_dic FOR SYSTEM_TIME AS OF t1.pt AS t4\n" +
                        "on t1.refund_reason_type = t4.dic_code"
        );

        //写出到对应的kafka主题
        tableEnv.executeSql(
                "create table "+ Constant.TOPIC_DWD_TRADE_ORDER_REFUND +" (\n" +
                        "\tid string ,\n" +
                        "\tuser_id string ,\n" +
                        "\tprovince_id string ,\n" +
                        "\torder_id string ,\n" +
                        "\tsku_id string ,\n" +
                        "\trefund_type_code string ,\n" +
                        "\trefund_type_name string ,\n" +
                        "\trefund_num string ,\n" +
                        "\trefund_amount string ,\n" +
                        "\trefund_reason_type_code string ,\n" +
                        "\trefund_reason_type_name string ,\n" +
                        "\trefund_reason_txt string ,\n" +
                        "\trefund_status string ,\n" +
                        "\tts bigint,\n" +
                        "\tPRIMARY KEY (id) NOT ENFORCED\n" +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND)
        );

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_REFUND).execute();

    }
}
