package com.matthew.gmall.realtime.common.util;

import com.matthew.gmall.realtime.common.constant.Constant;
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
    private static Connection connection;
    static {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建HBASE表格的方法
     * @param nameSpace
     * @param tableName
     * @param families
     * @throws IOException
     */
    public static void createHbaseTable(String nameSpace,String tableName,String ... families){
        Admin admin = null;
        try {
            if(families.length < 1){
                log.error("至少需要一个列族!");
                return;
            }
            admin = connection.getAdmin();
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
            log.info(tableName + "在HBASE中建表成功!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }




    }

    public static void dropHbaseTable(String nameSpace,String tableName){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableNm = TableName.valueOf(nameSpace, tableName);

            if(!admin.tableExists(tableNm)){
                log.warn(tableName + "在HBASE中不存在!!");
                return;
            }

            admin.disableTable(tableNm);
            admin.deleteTable(tableNm);
            log.info(tableName + "在HBASE中删除成功!!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



    }

}
