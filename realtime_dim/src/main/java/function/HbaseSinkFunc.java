package function;

import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.bean.TableProcessDim;
import com.matthew.gmall.realtime.common.constant.Constant;
import com.matthew.gmall.realtime.common.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * ClassName: HbaseSinkFunc
 * Package: function
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/3 22:56
 * @Version 1.0
 */
@Slf4j
public class HbaseSinkFunc implements SinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> dataWithConfig, Context context) throws Exception {
        //获取数据
        JSONObject data = dataWithConfig.f0;
        //获取数据的操作类型 d-c-r-u
        String op = data.getString("op");
        //获取维度表的配置
        TableProcessDim configDim = dataWithConfig.f1;
        String sinkTable = configDim.getSinkTable();
        //主键的名称 -> id
        String sinkRowKey = configDim.getSinkRowKey();
        //主键的值 -> 5
        String rowKeyValue = data.getString(sinkRowKey);
        if("d".equals(op)){
            //从HBASE中删除数据,要传主键的值，而不是主键的名称
            HBaseUtil.deleteRow(Constant.HBASE_NAMESPACE,sinkTable,rowKeyValue);
            log.info("从HBASE表:" + sinkTable + "中删除主键为:" + rowKeyValue + "的数据");
        }else{
            //添加数据，覆写操作insert+update
            HBaseUtil.putRow(Constant.HBASE_NAMESPACE,sinkTable,rowKeyValue,
                    configDim.getSinkFamily(),data);
            log.info("向HBASE表:" + sinkTable + "中添加主键为:" + rowKeyValue + "的数据");
        }

    }
}
