

import Gmall_fs.base.BaseApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import Gmall_fs.util.FlinkSourceUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @基本功能:   对日志数据进行处理并按类型进行分流
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-13 23:03:17
 *
 **/

//数据样本：
//    {
//        "actions":[{"action_id":"cart_add_num","item":"2","item_type":"sku_id","ts":1745671674000}],
//        "common":{"ar":"28","ba":"vivo","ch":"xiaomi","is_new":"1","md":"vivo x90","mid":"mid_167","os":"Android 13.0","sid":"17d8e5a8-c59f-4dfa-aaa3-8738bb07677b","uid":"1602","vc":"v2.1.134"},
//        "displays":[{"item":"16","item_type":"sku_id","pos_id":5,"pos_seq":0},{"item":"20","item_type":"sku_id","pos_id":5,"pos_seq":1},{"item":"17","item_type":"sku_id","pos_id":5,"pos_seq":2},{"item":"18","item_type":"sku_id","pos_id":5,"pos_seq":3},{"item":"6","item_type":"sku_id","pos_id":5,"pos_seq":4},{"item":"1","item_type":"sku_id","pos_id":5,"pos_seq":5},{"item":"34","item_type":"sku_id","pos_id":5,"pos_seq":6},{"item":"9","item_type":"sku_id","pos_id":5,"pos_seq":7},{"item":"21","item_type":"sku_id","pos_id":5,"pos_seq":8},{"item":"7","item_type":"sku_id","pos_id":5,"pos_seq":9},{"item":"16","item_type":"sku_id","pos_id":5,"pos_seq":10},{"item":"24","item_type":"sku_id","pos_id":5,"pos_seq":11},{"item":"18","item_type":"sku_id","pos_id":5,"pos_seq":12},{"item":"27","item_type":"sku_id","pos_id":5,"pos_seq":13},{"item":"18","item_type":"sku_id","pos_id":5,"pos_seq":14},{"item":"35","item_type":"sku_id","pos_id":5,"pos_seq":15},{"item":"2","item_type":"sku_id","pos_id":5,"pos_seq":16},{"item":"13","item_type":"sku_id","pos_id":5,"pos_seq":17}],
//        "page":{"during_time":17084,"last_page_id":"good_detail","page_id":"cart"},
//        "ts":1745671673000
//    }

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(
                1002,
                4,
                Constant.TOPIC_LOG,
                "dwd-log-consumer"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO 1、将读到的日志数据进行清洗转换
//        1.1   定义一个接收脏数据的侧道输出标签
        OutputTag<String> dirtyData = new OutputTag<>("dirty_data", TypeInformation.of(String.class));
//        1.2   对日志数据流进行清洗
        SingleOutputStreamOperator<JSONObject> process = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonstr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                尝试将接收到的日志数据转化为JsonObj，若转化失败，就作为脏数据放到侧道去
                try {
                    JSONObject jsonObject = JSONObject.parseObject(jsonstr);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyData, jsonstr);
                }
            }
        });

//        process.print("主流：");
//        process.getSideOutput(dirtyData).print("脏数据：");

//        1.3   脏数据侧道输出至kafka的脏数据topic中备用
        process.getSideOutput(dirtyData).sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));
//        1.4   对数据的is_new进行判定处理（is_new为新用户标记）
        SingleOutputStreamOperator<JSONObject> etledDS = process
                .keyBy( jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
//            定义一个键值状态，存放用户首次访问日期
            private ValueState<String> fristvisitdate;

            @Override
            public void open(Configuration parameters) throws Exception {
//                对状态进行初始化
                fristvisitdate = getRuntimeContext().getState(new ValueStateDescriptor<>("fristvisitdate", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                获取数据
                JSONObject common = jsonObject.getJSONObject("common");
//                获取新客标记
                String isNew = jsonObject.getString("is_new");
//                获取时间戳
                Long ts = jsonObject.getLong("ts");
//                进行判断
                if ("1".equals(isNew)) {
//                    当首次访问时间状态为空，说明该用户没有访问过，对状态进行更新、
                    if (fristvisitdate == null) {
                        fristvisitdate.update(DateFormatUtil.tsToDate(ts));
                    } else if ( !fristvisitdate.value().equals(DateFormatUtil.tsToDate(ts))
                                    &&
                                    DateFormatUtil.dateTimeToTs(fristvisitdate.value()) < ts) {
//                        若当前访问时间不等于状态时间且小于状态存储的时间时，判断为不是新客修改标签
                        common.put("is_new", "0");
                    }
                } else {
//                    当标签为0，但是状态又为空，说明标签标错，这是新客，修改标记并更新状态为当前时间的前一天
                    if (fristvisitdate == null) {
                        fristvisitdate.update(DateFormatUtil.tsToDate(ts - -24 * 60 * 60 * 1000));
                    }
                }
//                收集修改后数据
                out.collect(jsonObject);
            }
        });

//        TODO  2、数据分流
//        2.1   定义输出标签（错误日志、启动日志、曝光日志、动作日志）
        OutputTag<String> errTag = new OutputTag<String>("err"){};
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>(" display "){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
//        2.2   将数据流进行处理
        SingleOutputStreamOperator<String> pageDS = etledDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> collector) throws Exception {
//                2.2   取出数据里面的common数据
                JSONObject common = jsonObject.getJSONObject("common");
//                2.2   取出数据里的时间戳
                Long ts = jsonObject.getLong("ts");

//                2.2   启动页面：判断数据里是否有start这一键值，输出到start侧道去
                if (jsonObject.containsKey("start")) {
                    ctx.output(startTag, jsonObject.toJSONString());
                }

//                2.2   错误页面：判断数据里是否有err这一键值，输出到err侧道去
                if (jsonObject.containsKey("err")) {
                    ctx.output(errTag, jsonObject.toJSONString());
//                    将err这一键值对从数据里删除
                    jsonObject.remove("err");
                }

//                2.2   曝光页面：判断数据里是否有displays这一键值，输出到displays侧道去
                if (jsonObject.containsKey("displays")) {
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        display.putAll(common);
                        display.put("ts", ts);
                        ctx.output(displayTag, display.toJSONString());
                    }
                    jsonObject.remove("display");
                    }

                }
//                2.2   活动日志：判断数据里是否有action这一键值，输出到action测道去
                if (jsonObject.containsKey("actions")) {
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject action = actions.getJSONObject(i);
                        action.putAll(common);
                        ctx.output(actionTag, action.toJSONString());
                    }
                    jsonObject.remove("actions");
                }
//                2.2   处理完毕后把该页面日志送出
                if (jsonObject.containsKey("page")) {
                    collector.collect(jsonObject.toJSONString());
                }
            }
        }
        );
//        pageDS.getSideOutput(startTag).print("start：");
//        pageDS.getSideOutput(errTag).print("err：");
//        pageDS.getSideOutput(displayTag).print("display：");
//        pageDS.getSideOutput(actionTag).print("action：");

//        2.3   将侧道数据输出到各自的kafka主题上去
        pageDS.getSideOutput(startTag).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        pageDS.getSideOutput(errTag).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        pageDS.getSideOutput(displayTag).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        pageDS.getSideOutput(actionTag).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));


    }
}
