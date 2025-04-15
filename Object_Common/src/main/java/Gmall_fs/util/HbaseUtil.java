package Gmall_fs.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.doris.shaded.com.google.common.base.CaseFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HbaseUtil {

    /**
     * 开启hbase连接
     * @return hbase连接
     * @throws IOException
     */
    public static Connection createConnection() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection hbaseconnection = ConnectionFactory.createConnection(conf);
        return hbaseconnection;
    }

    /**
     * 关闭jbase连接
     * @param hbaseconnection hbase连接
     * @throws IOException
     */
    public static void closeConnection(Connection hbaseconnection) throws IOException {
//        判断连接是否存在
        if (hbaseconnection != null && !hbaseconnection.isClosed()) {
            hbaseconnection.close();
        }
    }

    /**
     * 创建表
     * @param hbaseconnection hbase连接
     * @param namepace 命名空间
     * @param tableName 表名
     * @param columnFamies 列族
     */
    public static void createTable(Connection hbaseconnection, String namepace,String tableName,String... columnFamies) {
        try(Admin admin = hbaseconnection.getAdmin()) {
//            判断是否输入了列族
            if (columnFamies.length < 1){
                System.out.println("请输入列族");
                return;
            }

            TableName tableName1 = TableName.valueOf(namepace, tableName);
//            判断表是否已经存在
            if(admin.tableExists(tableName1)) {
                System.out.println("表"+tableName+"已经存在");
                return ;
            }
//            创建表描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName1);
            for (String columnFamily : columnFamies) {
//                创建一个列族描述器，添加列族，在导入到表描述器中
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
//            通过表描述器创建表
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("命名空间"+namepace+"的表"+tableName+"成功创建");

        } catch (IOException e) {
           e.printStackTrace();
        }

    }

    /**
     * 删除表
     * @param hbaseconnection hbase连接
     * @param namepace 命名空间
     * @param tableName 表名
     */
    public static void dropTable(Connection hbaseconnection, String namepace,String tableName) {
        try(Admin admin = hbaseconnection.getAdmin()) {

            TableName tableName1 = TableName.valueOf(namepace, tableName);
//            判断表是否存在
            if(!admin.tableExists(tableName1)) {
                System.out.println("表不存在");
                return;
            }
//            将表改为弃用
            admin.disableTable(tableName1);
//            删除表
            admin.deleteTable(tableName1);
            System.out.println("命名空间"+namepace+"的表"+tableName+"成功删除");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * 向Hbase插入数据
     * @param hbaseconnection Hbase连接
     * @param namepace  命名空间
     * @param tableName 表名
     * @param rowkey    行键
     * @param columnFamily  列族
     * @param jsonObject 数据集合类
     */
    public static void putRow(Connection hbaseconnection, String namepace, String tableName, String rowkey ,String columnFamily, JSONObject jsonObject){
        TableName tableName1 = TableName.valueOf(namepace, tableName);
        try (Table table = hbaseconnection.getTable(tableName1)){

            Put put = new Put(Bytes.toBytes(rowkey));

            Set<String> columns = jsonObject.keySet();

            for (String column : columns) {
                if (jsonObject.getString(column)!=null) {
                    String value = jsonObject.getString(column);
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("成功向Hbase的"+namepace+"表"+tableName+"插入数据"+jsonObject);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 从Hbase删除数据
     * @param hbaseconnection hbase连接
     * @param namepace  命名空间
     * @param tableName 表名
     * @param rowkey        行键
     */
    public static void deleteRow(Connection hbaseconnection, String namepace, String tableName, String rowkey){
        TableName tableName1 = TableName.valueOf(namepace, tableName);
        try (Table table = hbaseconnection.getTable(tableName1)){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            System.out.println("成功从Hbase的"+namepace+"表"+tableName+"删除数据");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
 * 根据参数从 hbase 指定的表中查询一行数据
 *
 * @param hbaseConn hbase 链接
 * @param nameSpace 命名空间
 * @param table     表名
 * @param rowKey    rowKey
 * @return 把一行查询到的所有列封装到一个 JSONObject 对象中
 */
public static <T> T getRow(Connection hbaseConn,
                           String nameSpace,
                           String table,
                           String rowKey,
                           Class<T> tClass,
                           boolean... isUnderlineToCamel) {
    boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

    if (isUnderlineToCamel.length > 0) {
        defaultIsUToC = isUnderlineToCamel[0];
    }

    try (Table Table = hbaseConn.getTable(TableName.valueOf(nameSpace, table))) { // jdk1.7 : 可以自动释放资源
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = Table.get(get);
        // 4. 把查询到的一行数据,封装到一个对象中: JSONObject
        // 4.1 一行中所有的列全部解析出来
        List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
        T t = tClass.newInstance();
        for (Cell cell : cells) {
            // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (defaultIsUToC) { // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
            }
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            BeanUtils.setProperty(t, key, value);
        }
        return t;
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}


    /**
 * 获取到 Hbase 的异步连接
 *
 * @return 得到异步连接对象
 */

public static AsyncConnection getHBaseAsyncConnection() {
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", "node1");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    try {
        System.out.println("创建了一个hbase异步连接");
        return ConnectionFactory.createAsyncConnection(conf).get();
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}

/**
 * 关闭 hbase 异步连接
 *
 * @param asyncConn 异步连接
 */
public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
    if (asyncConn != null) {
        try {
            asyncConn.close();
            System.out.println("关闭了一个Hbase异步连接");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

/**
 * 异步的从 hbase 读取维度数据
 *
 * @param hBaseAsyncConn hbase 的异步连接
 * @param nameSpace      命名空间
 * @param tableName      表名
 * @param rowKey         rowKey
 * @return 读取到的维度数据, 封装到 json 对象中.
 */
public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                      String nameSpace,
                                      String tableName,
                                      String rowKey) {
    AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
        .getTable(TableName.valueOf(nameSpace, tableName));

    Get get = new Get(Bytes.toBytes(rowKey));
    try {
        // 获取 result
        Result result = asyncTable.get(get).get();
        List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
        JSONObject dim = new JSONObject();
        for (Cell cell : cells) {
            // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            dim.put(key, value);
        }

        return dim;

    } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
    }

}

}
