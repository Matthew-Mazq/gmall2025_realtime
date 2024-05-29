package com.matthew.gmall.realtime.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ClassName: HBaseUtil
 * Package: com.matthew.gmall.realtime.common.util
 * Description:   Hbase相关的工具类
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/29 22:48
 * @Version 1.0
 */
@Slf4j
public class HBaseUtil {
    /**
     * 获取HBASE的连接
     * @return
     * @throws IOException
     */
    public static Connection getHbaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(conf);
    }

    /**
     * 关闭HBASE的连接
     * @param connection
     * @throws IOException
     */
    public static void closeHbaseConnection(Connection connection) throws IOException {
        if(connection != null && !connection.isClosed()){
            connection.close();
        }
    }

    /**
     * 创建HBASE表格的方法
     * @param connection
     * @param nameSpace
     * @param tableName
     * @param families
     * @throws IOException
     */
    public static void createHbaseTable(Connection connection,String nameSpace,String tableName,String ... families) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableNm = TableName.valueOf(nameSpace, tableName);
        if(admin.tableExists(tableNm)){
            log.warn(tableName + "已经存在，不需要再建表!!");
            return;
        }
        //表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNm);
        //添加列族描述器
        for (String family : families) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        admin.createTable(tableDescriptorBuilder.build());

        admin.close();

        log.info(tableName + "在HBASE中建表成功!");
    }

    public static void dropHbaseTable(Connection connection,String nameSpace,String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableNm = TableName.valueOf(nameSpace, tableName);

        if(!admin.tableExists(tableNm)){
            log.warn(tableName + "在HBASE中不存在!!");
        }

        admin.disableTable(tableNm);
        admin.deleteTable(tableNm);
        admin.close();

        log.info(tableName + "在HBASE中删除成功!!");
    }

}
