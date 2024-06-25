package com.matthew.gmall.realtime.dws.app;

import com.matthew.gmall.realtime.common.base.BaseSQLAPP;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.SQLUtil;
import com.matthew.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.matthew.gmall.realtime.dws.app
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/23 12:23
 * @Version 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10020,4,"DwsTrafficSourceKeywordPageViewWindow");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //1.读取DWD页面主题数据并设置水位线
        tableEnv.executeSql(
                "create table page_log(\n" +
                        "\tpage map<string,string>,\n" +
                        "\tcommon map<string,string>,\n" +
                        "\tts bigint,\n" +
                        "\tet as to_timestamp_ltz(ts,3),\n" +
                        "\tWATERMARK FOR et as et - INTERVAL '5' SECOND\n" +
                        ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRAFFIC_PAGE,"DwsTrafficSourceKeywordPageViewWindow")
        );

        // tableEnv.executeSql("select * from page_log").print();
        //2.过滤搜索行为
        Table kwTable = tableEnv.sqlQuery(
                "select \n" +
                        "\tpage['item'] keywords,\n" +
                        "\tet\n" +
                        "from page_log\n" +
                        "where page['item'] is not null \n" +
                        "and page['last_page_id'] is not null \n" +
                        "and page['item_type'] = 'keyword'"
        );
        tableEnv.createTemporaryView("kw_table",kwTable);

        //3.注册自定义函数-分词
        tableEnv.createTemporaryFunction("kw_split", KwSplit.class);

        //4.分词
        Table keywordTable = tableEnv.sqlQuery(
                "select \n" +
                        "\tkeyword,\n" +
                        "\tet\n" +
                        "from kw_table\n" +
                        "LEFT JOIN LATERAL TABLE(kw_split(keywords)) ON TRUE"
        );
        tableEnv.createTemporaryView("keyword_table",keywordTable);

        //5.开窗分组聚合
        Table resultTable = tableEnv.sqlQuery(
                "select \n" +
                        "date_format(window_start,'yyyy-MM-dd HH:mm:ss') stt,\n" +
                        "date_format(window_end,'yyyy-MM-dd HH:mm:ss') edt,\n" +
                        "date_format(window_start,'yyyyMMdd') cur_date,\n" +
                        "keyword,\n" +
                        "count(1) as keyword_count\n" +
                        "FROM TABLE(\n" +
                        "   TUMBLE(TABLE keyword_table, DESCRIPTOR(et), INTERVAL '5' SECOND))\n" +
                        "group by window_start,window_end,keyword"
        );

        //6.写入StarRocks
        tableEnv.executeSql(
                "CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                        "    stt string,\n" +
                        "    edt string,\n" +
                        "    cur_date string,\n" +
                        "    keyword string,\n" +
                        "\tkeyword_count bigint\n" +
                        ") WITH (\n" +
                        "    'connector' = 'starrocks',\n" +
                        "    'jdbc-url' = 'jdbc:mysql://"+ Constant.STARROCKS_FE_IP +":9030',\n" +
                        "    'load-url' = '"+ Constant.STARROCKS_FE_IP +":8030',\n" +
                        "    'database-name' = 'gmall2023_realtime',\n" +
                        "    'table-name' = 'dws_traffic_source_keyword_page_view_window',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = '123456',\n" +
                        "\t'sink.semantic' = 'exactly-once',\n" +
                        "\t'sink.label-prefix' = 'dws_page',\n" +
                        "\t'sink.properties.format' = 'json'\n" +
                        ");"
        );
        resultTable.executeInsert("dws_traffic_source_keyword_page_view_window");



    }
}
