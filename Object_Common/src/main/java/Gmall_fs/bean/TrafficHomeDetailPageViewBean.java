package Gmall_fs.bean;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 14:28:42
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}