package me.iroohom.util;

import me.iroohom.config.QuotConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

/**
 * @ClassName: HbaseUtil
 * @Author: Roohom
 * @Function: Hbase工具类，HBase的常用操作
 * @Date: 2020/10/31 10:38
 * @Software: IntelliJ IDEA
 */
public class HbaseUtil {

    static Connection connection = null;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", QuotConfig.config.getProperty("zookeeper.connect"));
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取表
    public static Table getTable(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }


    /**
     * 插入单列数据
     *
     * @param tableName 表名
     * @param family    列族
     * @param colName   列名
     * @param colValue  列值
     * @param rowkey    rowkey
     */
    public static void putDataByRowkey(String tableName, String family, String colName, String colValue, String rowkey) {
        Table table = getTable(tableName);
        try {
            Put put = new Put(rowkey.getBytes());
            put.addColumn(family.getBytes(), colName.getBytes(), colValue.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 插入多列数据
     *
     * @param tableName 表名
     * @param family    列族
     * @param map       多列值
     * @param rowkey    rowkey
     */
    public static void putMapDataByRowkey(String tableName, String family, Map<String, Object> map, String rowkey) {
        Table table = getTable(tableName);
        try {
            Put put = new Put(rowkey.getBytes());

            //封装Map中的数据到put对象中
            for (String key : map.keySet()) {
                put.addColumn(family.getBytes(), key.getBytes(), map.get(key).toString().getBytes());
            }

            //执行put
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关闭表
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * 根据rowkey查询数据
     *
     * @param tableName 表名
     * @param family    列族
     * @param colName   列名
     * @param rowkey    rowkey
     * @return
     */
    public static String queryByRowkey(String tableName, String family, String colName, String rowkey) {
        Table table = getTable(tableName);
        String string = null;
        try {
            Get get = new Get(rowkey.getBytes());
            Result result = table.get(get);
            byte[] value = result.getValue(family.getBytes(), colName.getBytes());
            string = Arrays.toString(value);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return string;
    }


    /**
     * 根据rowkey删除数据
     *
     * @param tableName 表名
     * @param family    列族
     * @param rowkey    rowkey
     */
    public static void delByRowkey(String tableName, String family, String rowkey) {

        Table table = getTable(tableName);
        try {
            Delete delete = new Delete(rowkey.getBytes());
            delete.addFamily(family.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关表
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 批量插入数据
     *
     * @param tableName 表名
     * @param list      封装put对象的list
     */
    public static void putList(String tableName, List<Put> list) {
        Table table = getTable(tableName);
        try {
            table.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        /**
         *  3.插入单列数据
         *  4.插入多列数据
         *  5.根据rowkey查询数据
         *  6.根据rowkey删除数据
         *  7.批量数据插入
         */

        /**
         * 创建表：create 'test','f1'
         */

        //3.插入单列数据
        putDataByRowkey("test", "f1", "name", "xiaoli", "1");

//
//        //4.插入多列数据
//        HashMap<String, Object> map = new HashMap<>();
//        map.put("name", "xiaozhang");
//        map.put("age", 20);
//        putMapDataByRowkey("test", "f1", map, "2");
//
//        //5.根据rowkey查询数据
//        String str = queryByRowkey("test", "f1", "name", "1");
//        System.out.println("<<<<:" + str);
//
//        //6.根据rowkey删除数据
////        delByRowkey("test","f1","2");
//
        //7.批量数据插入
        ArrayList<Put> list = new ArrayList<>();
        Put put = new Put("3".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "xiaohong".getBytes());
        Put put2 = new Put("4".getBytes());
        put2.addColumn("f1".getBytes(), "name".getBytes(), "xiaowang".getBytes());
        list.add(put);
        list.add(put2);
        putList("test", list);
    }


}
