import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TableProcessDim;
import Gmall_fs.bean.TableProcessDwd;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSinkUtil;
import Gmall_fs.util.FlinkSourceUtil;
import Gmall_fs.util.JdbcUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-16 08:47:08
 **/

public class DwdBaseDb extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseDb().start(
                10019,
                4,
                Constant.TOPIC_DB,
                "dwd_base_db"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//          业务数据jsonStr
//          {
//              "database":"gmall",
//              "table":"coupon_use",
//              "type":"insert",
//              "ts":1744810644,
//              "xid":594715,
//              "xoffset":0,
//              "data":{
//                              "id":3726,
//                              "coupon_id":1,
//                              "user_id":1168,"order_id":null,"coupon_status":"1401","get_time":"2024-06-08 21:37:24","using_time":null,"used_time":null,"expire_time":null,"create_time":"2024-06-08 21:37:24","operate_time":null
//                          }
//          }

//        TODO  将jsonStr转化为jsonObj
        SingleOutputStreamOperator<JSONObject> etlStream = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            return jsonObject;
                        } catch (Exception e) {
                            throw new RuntimeException("不是正常的json字符串");
                        }
                    }
                }
        ).setParallelism(1);

//        TODO  读取配置流信息
        MySqlSource<String> MySqlSource = FlinkSourceUtil.getMySqlSource("gmall_config","table_process_dwd") ;

//        TODO  对配置流进行转换成流
        DataStreamSource<String> msource = env.fromSource(MySqlSource, WatermarkStrategy.noWatermarks(), "MySQLsource");
//        {
//          "before":null,
//          "after":{"source_table":"user_info","source_type":"insert","sink_table":"dwd_user_register","sink_columns":"id,create_time"},
//          "source":{
//                          "version":"1.9.7.Final",
//                          "connector":"mysql",
//                          "name":"mysql_binlog_source",
//                          "ts_ms":0,
//                          "snapshot":"false",
//                          "db":"gmall_config",
//                          "sequence":null,
//                          "table":"table_process_dwd",
//                          "server_id":0,
//                          "gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null
//                          },
//          "op":"r",
//          "ts_ms":1744813448269,
//          "transaction":null}

        SingleOutputStreamOperator<TableProcessDwd> tpDS = msource.map(new RichMapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
//                        将json字符串转换成Json实体类
                JSONObject jsonObject = JSON.parseObject(jsonStr);
//                        提取出操作类型
                String op = jsonObject.getString("op");
                TableProcessDwd tableProcessDwd;
//                        判断是否为删除操作
                if (op.equals("d")) {
                    tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                } else {
                    tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                }

                tableProcessDwd.setOp(op);
                return tableProcessDwd;
            }
        });

//        TODO  创建广播流描述器
        MapStateDescriptor<String, TableProcessDwd> broadcastDs = new MapStateDescriptor<>("broadcastDs", String.class, TableProcessDwd.class);

//        TODO  将配置流转化为广播流
        BroadcastStream<TableProcessDwd> broadcast = tpDS.broadcast(broadcastDs);

//        TODO  主流联合广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream =
        etlStream.connect(broadcast).process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>() {

            private HashMap<String, TableProcessDwd> map;

            @Override
            public void open(Configuration parameters) throws Exception {
                // open 中没有办法访问状态!!!
                        map = new HashMap<>();
                        // 1. 去 mysql 中查询 table_process 表所有数据
                        java.sql.Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mysqlConn,
                                "select * from gmall_config.table_process_dwd",
                                TableProcessDwd.class,
                                true
                        );

                        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                            String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
                            map.put(key, tableProcessDwd);
                        }
                        JdbcUtil.closeConnection(mysqlConn);
            }

            @Override
            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext context, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                ReadOnlyBroadcastState<String, TableProcessDwd> state = context.getBroadcastState(broadcastDs);
                        String key = getKey(jsonObj.getString("table"), jsonObj.getString("type"));
                        TableProcessDwd tableProcessDwd = state.get(key);

                        if (tableProcessDwd == null) {  // 如果状态中没有查到, 则去 map 中查找
                            tableProcessDwd = map.get(key);

                        if (tableProcessDwd != null) { // 这条数据找到了对应的配置信息
                            JSONObject data = jsonObj.getJSONObject("data");
                            out.collect(Tuple2.of(data, tableProcessDwd));
                        }

            }

        }

            @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        BroadcastState<String, TableProcessDwd> state = context.getBroadcastState(broadcastDs);
                        String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());

                        if ("d".equals(tableProcessDwd.getOp())) {
                            // 删除状态
                            state.remove(key);
                            // map中的配置也要删除
                            map.remove(key);
                        } else {
                            // 更新或者添加状态
                            state.put(key, tableProcessDwd);
                        }
                    }

            private String getKey(String source_table, String sourceType) {
        return source_table + ":" + sourceType;
                 }

        });

//        TODO  将其写到kafka上
        dataWithConfigStream.sinkTo(FlinkSinkUtil.getKafkaSink());

    }



}
