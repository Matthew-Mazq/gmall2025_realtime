package com.matthew.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * ClassName: SRMapFunction
 * Package: com.matthew.gmall.realtime.common.function
 * Description: 工具类，用于完成小驼峰至下划线命名的转换
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/26 21:58
 * @Version 1.0
 */
public class SRMapFunction<T> implements MapFunction<T,String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig conf = new SerializeConfig();
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(value,conf);
    }
}
