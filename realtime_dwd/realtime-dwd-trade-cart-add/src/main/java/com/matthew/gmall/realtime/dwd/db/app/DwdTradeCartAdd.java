package com.matthew.gmall.realtime.dwd.db.app;

import com.matthew.gmall.realtime.common.base.BaseSQLAPP;
import com.matthew.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeCartAdd
 * Package: com.matthew.gmall.realtime.dwd.db.app
 * Description: 交易域加购事务事实表
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/15 11:50
 * @Version 1.0
 */
public class DwdTradeCartAdd extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //1.读取topic_db中的数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);

        //2.从中过滤出加购的事实数据
        tableEnv.executeSql(
                "select \n" +
                        "\tafter['id'] as id,\n" +
                        "\tafter['user_id'] as user_id,\n" +
                        "\tafter['sku_id'] as sku_id,\n" +
                        "\tafter['cart_price'] as cart_price,\n" +
                        "\tif(\n" +
                        "\t\t'op'='u',\n" +
                        "\t\tcast(after['sku_num'] as bigint) - cast(before['sku_num'] as bigint),\n" +
                        "\t\tcast(after['sku_num'] as bigint)\n" +
                        "\t) as sku_num,\n" +
                        "\tafter['sku_name'] as sku_name,\n" +
                        "\tafter['create_time'] as create_time,\n" +
                        "\tafter['operate_time'] as operate_time,\n" +
                        "\tts_ms as ts\n" +
                        "from topic_db\n" +
                        "where source['db'] = 'gmall' and source['table'] = 'cart_info'\n" +
                        "and (\n" +
                        "\top in ('c','r') or (op = 'u' and before['sku_num'] is not null \n" +
                        "\t\t\t\t\t\tand cast(after['sku_num'] as bigint) > cast(before['sku_num'] as bigint))\n" +
                        "\t)"
        ).print();
    }
}
