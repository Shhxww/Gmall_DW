package Gmall_fs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
                System.out.println("表已经存在");
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


}
