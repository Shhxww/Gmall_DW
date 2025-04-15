package Gmall_fs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 20:11:52
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 注册用户数
    Long registerCt;

}
