package Gmall_fs.bean;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-19 17:04:58
 **/

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrafficPageViewBean {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String cur_date;
    // app 版本号
    private String vc;
    // 渠道
    private String ch;
    // 地区
    private String ar;
    // 新老访客状态标记
    private String isNew ;

    // 独立访客数
    private Long uvCt;
    // 会话数
    private Long svCt;
    // 页面浏览数
    private Long pvCt;
    // 累计访问时长
    private Long durSum;
    // 时间戳
    @JSONField(serialize = false)  // 要不要序列化这个字段
    private Long ts;
    @JSONField(serialize = false)  // 要不要序列化这个字段
    private String sid;
}