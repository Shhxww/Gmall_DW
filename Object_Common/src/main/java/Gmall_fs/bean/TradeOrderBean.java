package Gmall_fs.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 21:44:00
 **/

@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    // 当天日期
    String curDate;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}