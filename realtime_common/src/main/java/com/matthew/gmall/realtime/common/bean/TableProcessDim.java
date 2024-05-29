package com.matthew.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TableProcessDim
 * Package: com.matthew.gmall.realtime.common.bean
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/29 22:20
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    // sink到 hbase 的时候的主键字段
    String sinkRowKey;

    // 配置表操作类型
    String op;

}

