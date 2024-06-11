package function;

import com.alibaba.fastjson.JSONObject;
import com.matthew.gmall.realtime.common.bean.TableProcessDim;
import com.matthew.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

/**
 * ClassName: connectProcFunc
 * Package: function
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/5/31 17:36
 * @Version 1.0
 */
public class connectProcFunc extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> configState;

    public connectProcFunc(MapStateDescriptor<String, TableProcessDim> configState) {
        this.configState = configState;
    }

    HashMap<String, TableProcessDim> configMap = new HashMap();

    @Override
    public void open(Configuration parameters) throws Exception {
        //通过jdbc把配置表的信息预加载到一个map集合中
        Connection connection = JdbcUtil.getMysqlConnection();
        String querySql = "select * from gmall2023_config.table_process_dim";
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(connection, querySql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            tableProcessDim.setOp("r");
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(connection);

    }

    /**
     * 处理主流中的数据
     * @param jsonObj The stream element.
     * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *     querying the current processing/event time and updating the broadcast state. The context
     *     is only valid during the invocation of this method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(configState);
        TableProcessDim tableProcessDim;
        //获取主流数据的来源表名
        String sourceTable = jsonObj.getJSONObject("source").getString("table");
        //获取操作类型
        String op = jsonObj.getString("op");
        //判断是否包含该表名
        tableProcessDim = !broadcastState.contains(sourceTable) ? broadcastState.get(sourceTable) : configMap.get(sourceTable);
        if (tableProcessDim != null) {
            //({"birthday":"1991-06-07","op":"u","gender":"M","create_time":"2022-06-08 00:00:00","login_name":"vihcj30p1","nick_name":"豪心","name":"魏豪心","user_level":"1","phone_num":"13956932645","id":7,"email":"vihcj30p1@live.com"}
            // ,TableProcessDim(sourceTable=user_info, sinkTable=dim_user_info, sinkColumns=id,login_name,name,user_level,birthday,gender,create_time,operate_time, sinkFamily=info, sinkRowKey=id, op=r))
            //如果是删除操作，则把before的数据放入out对象中，使得最后在输出到HBASE，调用SinkFunction时，能够获取到主键调用delete方法
            if ("d".equals(op)) {
                JSONObject beforeJsonObj = jsonObj.getJSONObject("before");
                beforeJsonObj.put("op", op);
                out.collect(Tuple2.of(beforeJsonObj, tableProcessDim));
            } else {
                JSONObject afterJsonObj = jsonObj.getJSONObject("after");
                afterJsonObj.put("op", op);
                out.collect(Tuple2.of(afterJsonObj, tableProcessDim));
            }
        }
    }


    //处理广播流的数据
    @Override
    public void processBroadcastElement(TableProcessDim dim, Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(configState);
        //根据op操作来更新广播中的状态以及更新map
        String op = dim.getOp();
        if ("d".equals(op)) {
            broadcastState.remove(dim.getSourceTable());
            configMap.remove(dim.getSourceTable());
        } else {
            broadcastState.put(dim.getSourceTable(), dim);
            configMap.put(dim.getSourceTable(), dim);
        }
    }
}
