package Gmall_fs.base;

import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSourceUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public abstract class BaseApp {
    /**
     * 启动程序
     * @param port 测试的web界面端口
     * @param parallelism   程序并行度
     * @param kafka_topic   kafka的topic
     * @param kafka_groupid kafka的消费者组名
     * @throws Exception
     */
    public void start(int port ,int parallelism ,String kafka_topic, String kafka_groupid)  {
 //        TODO 1、 设置初始环境
//        1.1 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        1.2 设置程序全局并行度
        env.setParallelism(parallelism);
//        1.3   设置设置操作 Hadoop 的用户名为 Hadoop 超级用户 root
        System.setProperty("HADOOP_USER_NAME", "root");

//        TODO 2、 配置检查点、重启策略
//        2.1   启用检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        2.2   设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        2.3   设置检查点间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        2.4   设置检查点在任务结束后进行保存，及其保存路径(保存在hdfs上要指定有权限的用户)
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node1:8020/Flink_checkpoint/");
//        2.5   设置重启策略（每3s重启一次，30天内仅能重启三次）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.days(3),Time.seconds(3)));
//        TODO  3、读取kafka上的业务数据
//        3.1   创建一个kafka对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getkafkaSource(kafka_topic,kafka_groupid);

//        3.2   封装成流
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaDS");

//        处理逻辑
        handle(env,kafkaDS);

//        执行程序
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 处理函数
     * @param env Flink流数据运行环境
     * @param  kafkaDS kafka上读取的数据
     */
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS);

}
