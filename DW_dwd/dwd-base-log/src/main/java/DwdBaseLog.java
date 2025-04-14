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

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(1002,4,Constant.TOPIC_LOG,"dwd-log-consumer");
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
        process.print("主流：");
        process.getSideOutput(dirtyData).print("脏数据：");

//        1.3   脏数据侧道输出至kafka的脏数据topic中备用
        process.getSideOutput(dirtyData).sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));
//        1.4   对数据的is_new进行判定处理
        SingleOutputStreamOperator<JSONObject> etledDS = process.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<String> fristvisitdate;

            @Override
            public void open(Configuration parameters) throws Exception {

                fristvisitdate = getRuntimeContext().getState(new ValueStateDescriptor<>("fristvisitdate", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                String isNew = jsonObject.getString("is_new");
                Long ts = jsonObject.getLong("ts");

                if ("1".equals(isNew)) {
                    if (fristvisitdate == null) {
                        fristvisitdate.update(DateFormatUtil.tsToDate(ts));
                    } else if (
                            !fristvisitdate.value().equals(DateFormatUtil.tsToDate(ts))
                                    &&
                                    DateFormatUtil.dateTimeToTs(fristvisitdate.value()) < ts) {
                        common.put("is_new", "0");
                    }
                } else {
                    if (fristvisitdate == null) {
                        fristvisitdate.update(DateFormatUtil.tsToDate(ts - -24 * 60 * 60 * 1000));
                    }
                }

                out.collect(jsonObject);
            }
        });

//        TODO  2、数据分流
//        2.1   定义输出标签
        OutputTag<String> errTag = new OutputTag<String>("err"){};
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
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



    }
}
