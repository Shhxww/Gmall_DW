package Gmall_fs.function;
import com.alibaba.fastjson.JSONObject;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-22 11:12:11
 **/



public interface DimFunction<T> {
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}