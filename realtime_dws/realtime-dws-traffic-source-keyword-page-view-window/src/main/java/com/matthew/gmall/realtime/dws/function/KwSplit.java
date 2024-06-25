package com.matthew.gmall.realtime.dws.function;

import com.matthew.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * ClassName: KwSplit
 * Package: com.matthew.gmall.realtime.dws.function
 * Description:  自定义FlinkSQL的UDTF函数，实现分词
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/23 12:29
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String keyWords){
        if (keyWords == null){
            return;
        }
        Set<String> wordSet = IkUtil.split(keyWords);
        for (String keyword : wordSet) {
            collect(Row.of(keyword));
        }
    }
}
