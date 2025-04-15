package dws.app;

import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.streaming.api.datastream.DataStreamUtils.collect;

/**
 * @基本功能:   流量域---搜索关键词---页面浏览各窗口汇总表
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-19 09:18:33
 **/

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

//        TODO  读取页面日志数据，转化为动态
//        {
    //        "common":{"ar":"28","uid":"1227","os":"Android 13.0","ch":"web","is_new":"1","md":"vivo IQOO Z6x ","mid":"mid_62","vc":"v2.1.132","ba":"vivo","sid":"190c1a34-1343-41a4-ba96-670772dfb402"},
    //        "page":{"page_id":"order","item":"20","during_time":14101,"item_type":"sku_ids","last_page_id":"good_detail"},
    //        "ts":1717856903000
//        }

        tEnv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second " +
                ")"+ FlinkSQlUtil.getKafkaDDLSource(
                        Constant.TOPIC_DWD_TRAFFIC_PAGE,"page_log")
        );


//        TODO  读取搜索关键词
        Table kwTable = tEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tEnv.createTemporaryView("kw_table", kwTable);

//        TODO  自定义分词函数
        tEnv.createTemporaryFunction("kw_split",SplitFunction.class);
        Table keywordTable = tEnv.sqlQuery(
                "select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true ");

        tEnv.createTemporaryView("keyword_table", keywordTable);

//        TODO  开窗聚和 tvf
        Table result = tEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyyMMdd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '5' second ) ) " +
                "group by window_start, window_end, keyword ");

//        TODO  写出到 doris 中
        tEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = '000000', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window");


    }


    @FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
    public static class SplitFunction extends TableFunction<Row> {
      public void eval(String kw) {
          if (kw==null)return;
          List<String> words = IkUtil.analyzeSplit(kw);
          for (String keyword : words) {
              collect(Row.of(keyword));
          }
      }
}



}

