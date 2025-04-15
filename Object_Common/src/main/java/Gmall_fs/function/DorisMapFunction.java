package Gmall_fs.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @基本功能：将数据写入Doris前，先转化为JsonStr
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-20 10:18:35
 **/
public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        SerializeConfig conf = new SerializeConfig();
//        设置蛇形命名法
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, conf);
    }
}