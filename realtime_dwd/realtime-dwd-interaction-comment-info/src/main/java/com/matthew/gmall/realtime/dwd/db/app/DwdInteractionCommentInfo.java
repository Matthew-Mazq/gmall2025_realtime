package com.matthew.gmall.realtime.dwd.db.app;

import com.matthew.gmall.realtime.common.base.BaseSQLAPP;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.utils.DateTimeUtils;

import java.time.Duration;

/**
 * ClassName: DwdInteractionCommentInfo
 * Package: com.matthew.gmall.realtime.dwd.db.app
 * Description: 互动域评论事务事实表
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/15 10:55
 * @Version 1.0
 */
public class DwdInteractionCommentInfo extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //1.从topic_db中读取数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        //状态过期时间为5秒
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        //2.从中过滤出评论表的数据
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "\tafter['id'] as id,\n" +
                "\tafter['user_id'] as user_id,\n" +
                "\tafter['nick_name'] as nick_name,\n" +
                "\tafter['sku_id'] as sku_id,\n" +
                "\tafter['spu_id'] as spu_id,\n" +
                "\tafter['order_id'] as order_id,\n" +
                "\tafter['appraise'] as appraise,\n" +
                "\tafter['comment_txt'] as comment_txt,\n" +
                "\tafter['create_time'] as create_time,\n" +
                "\tafter['operate_time'] as operate_time,\n" +
                "\tts_ms as ts,\n" +
                "\tpt\n" +
                "from topic_db\n" +
                "where source['db'] = 'gmall' and source['table'] = 'comment_info'\n" +
                "and op in ('c','r','u')");
        tableEnv.createTemporaryView("comment_info",commentInfo);

        //3.从HBASE表中读取base_dic数据
        readBaseDic(tableEnv);

        //4.评论表与base_dic 进行look_up join
        Table joinTable = tableEnv.sqlQuery(
                "select \n" +
                        "t1.id,\n" +
                        "t1.user_id,\n" +
                        "t1.sku_id,\n" +
                        "t1.appraise,\n" +
                        "t2.info.dic_name as appraise_name,\n" +
                        "t1.comment_txt,\n" +
                        "t1.ts\n" +
                        "from comment_info as t1\n" +
                        "join base_dic FOR SYSTEM_TIME AS OF t1.pt AS t2\n" +
                        "on t1.appraise = t2.dic_code"
        );

        //5.建立kafka的Sink
        tableEnv.executeSql(
                "create table "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +"(\n" +
                        "id string,\n" +
                        "user_id string,\n" +
                        "sku_id string,\n" +
                        "appraise string,\n" +
                        "appraise_name string,\n" +
                        "comment_txt string,\n" +
                        "ts bigint\n" +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );

        //6.把关联的结果写入kafka中
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();

    }
}
