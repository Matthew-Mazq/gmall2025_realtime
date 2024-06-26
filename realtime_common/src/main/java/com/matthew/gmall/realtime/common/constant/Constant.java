package com.matthew.gmall.realtime.common.constant;

/**
 * ClassName: Constant
 * Package: com.matthew.gmall.realtime.common.constant
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/28 22:11
 * @Version 1.0
 */
public class Constant {
    public static final String KAFKA_BROKERS = "node1:9092,node2:9092,node3:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "node1";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String ODS_TABLE_LIST = "gmall.*";
    public static final String CONFIG_DWD_DATABASE = "gmall2023_config";
    public static final String CONFIG_DWD_TABLE_LIST = "gmall2023_config.table_process_dwd";
    public static final String HBASE_NAMESPACE = "gmall";
    public static final String HBASE_ZOOKEEPER_QUORUM ="node1,node2,node3";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://node1:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String STARROCKS_FE_IP = "node1";
    public static final String STARROCKS_DATABASES = "gmall2023_realtime";

}
