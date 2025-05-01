import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TableProcessDim;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSourceUtil;
import Gmall_fs.util.HbaseUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.val;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.http.client.RedirectStrategy;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.IOException;
import java.util.Properties;

/**
 * @基本功能:   实时将dim维度表，表数据更新至Hbase
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-13 23:03:17
 **/

public class DimAPP extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new DimAPP().start(
                10011,
                4,
                Constant.TOPIC_DB,
                "dim_app_group"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO   1、将Json字符串数据流进行转换成jsonObj主流
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonstr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonstr);
                return jsonObject;
            }
        });

//        TODO  2、读取维度表配置流
//        配置mysqlCDC
        MySqlSource<String> gmallConfig = FlinkSourceUtil.getMySqlSource("gmall_config","table_process_dwd") ;

//        封装成流(并行度要设置成1，不然会导致配置流出现乱序，无法同步)
        DataStreamSource<String> gmall_config = env.fromSource(gmallConfig, WatermarkStrategy.noWatermarks(), "Gmall_config").setParallelism(1);

//        将读取到的配置数据进行转换（设置并行度为1）
        SingleOutputStreamOperator<TableProcessDim> tpDS = gmall_config.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
//                        将json字符串转换成Json实体类
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
//                        提取出操作类型
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim;
//                        判断是否为删除操作
                        if (op.equals("d")) {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
//                        设置维度表更新的操作类型
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

//        TODO  3、根据配置流在Hbase中 创建 / 删除 维度表
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
//                    创建HBase连接
                    private Connection hbaseConnection = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        获取HBase连接
                        hbaseConnection = HbaseUtil.createConnection();
                    }

                    @Override
                    public void close() throws Exception {
//                        程序结束，关闭HBase连接
                        HbaseUtil.closeConnection(hbaseConnection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
//                        获取操作类型
                        String op = tableProcessDim.getOp();
//                        获取操作表名
                        String sinkTable = tableProcessDim.getSinkTable();
//                        获取输出字段
                        String[] columnS = tableProcessDim.getSinkFamily().split(",");
//                        若操作类型为删除则删除目标维度表、
//                        若操作类型为写入或创建，则创建维度表、
//                        若操作为更新则删除维度表再进行创建
                        if (op.equals("d")) {
                            HbaseUtil.dropTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                        } else if (op.equals("r") || op.equals("c")) {
                            HbaseUtil.createTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, columnS);
                        } else if (op.equals("u")) {
                            HbaseUtil.dropTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                            HbaseUtil.createTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, columnS);
                        }
                        return tableProcessDim;
                    }
                }
                );

//        TODO  4、  将配置流转换为广播流
//        创建一个广播流描述器
        MapStateDescriptor<String, TableProcessDim> broadcastDs = new MapStateDescriptor<>("broadcastDs", String.class, TableProcessDim.class);
//        创建广播流
        BroadcastStream<TableProcessDim> broadcast = tpDS.broadcast(broadcastDs);

//        TODO  5、  将广播流和主流进行合并，并进行筛选出维度表数据放入下游
        SingleOutputStreamOperator<Tuple3<String, JSONObject, TableProcessDim>> dimDS = jsonObj.connect(broadcast)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple3<String, JSONObject, TableProcessDim>>() {

//            从业务数据流过滤出维度表的数据
            @Override
            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple3<String, JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple3<String, JSONObject, TableProcessDim>> collector) throws Exception {
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(broadcastDs);
                String table = jsonObject.getString("table");

                TableProcessDim tableProcessDim = broadcastState.get(table);
//                在广播状态里查询是否存在，若存在，就表示这是维度表的数据
                if (tableProcessDim != null) {
                    JSONObject data = jsonObject.getJSONObject("data");
//                    判断data里键值对是否为0，类型为bootstrap-start/end 的data为{}，不判断就无法插入
                    if (!data.isEmpty()) {
                        collector.collect(Tuple3.of(jsonObject.getString("type"), data, tableProcessDim));
                    }
                }
            }

            /**
             * 对广播流进行处理，将配置信息放入广播状态中去
             * @param tableProcessDim   维度表配置类
             * @param context
             * @param collector
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple3<String, JSONObject, TableProcessDim>>.Context context, Collector<Tuple3<String, JSONObject, TableProcessDim>> collector) throws Exception {
                BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(broadcastDs);
                String op = tableProcessDim.getOp();
                if (op.equals("d")) {
//                    若操作类型为删除，则从广播状态中删除这条状态
                    broadcastState.remove(tableProcessDim.getSourceTable());
                } else {
//                    若操作类型为添加、更改、读取，则从广播状态中添加这条状态
                    broadcastState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                }
            }
        });

//        TODO  6、  根据维度表数据流对Hbase进行维度表操作
        dimDS.addSink(new RichSinkFunction<Tuple3<String, JSONObject, TableProcessDim>>() {

            private Connection hbaseConnection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConnection = HbaseUtil.createConnection();
            }

            @Override
            public void close() throws Exception {
                HbaseUtil.closeConnection(hbaseConnection);
            }

            @Override
            public void invoke(Tuple3<String, JSONObject, TableProcessDim> tp3, Context context) throws Exception {
//                获取操作类型
                String type = tp3.f0;
//                获取数据
                JSONObject jsonObject = tp3.f1;
//                获取维度表配置类
                TableProcessDim tableProcessDim = tp3.f2;
//                获取插入的rowkey
                String rowkey = jsonObject.getString(tableProcessDim.getSinkRowKey());
//                获取目标命名空间
                String namespace = Constant.HBASE_NAMESPACE;
//                获取目标表名
                String tableName = tableProcessDim.getSinkTable();
                if ("delete".equals(type)) {
//                    若操作类型为删除，则删除Hbase中维度表的这条数据
                    HbaseUtil.deleteRow(hbaseConnection,namespace, tableName,rowkey);
                }else {
//                    若操作类型不为删除（添加，更改，读取），则向Hbase维度表中添加这条数据
                    HbaseUtil.putRow(hbaseConnection,namespace,tableName,rowkey,tableProcessDim.getSinkFamily(),jsonObject);
                }
            }
        });

    }
}
