package com.matthew.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * ClassName: TableProcessDwd
 * Package: com.matthew.gmall.realtime.common.bean
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/16 18:14
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd implements Serializable {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}

