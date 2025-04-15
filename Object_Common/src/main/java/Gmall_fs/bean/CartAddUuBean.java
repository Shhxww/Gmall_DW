package Gmall_fs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 20:38:54
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}