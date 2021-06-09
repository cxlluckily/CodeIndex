package me.iroohom.util;

//import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName: HbaseUtil
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/9 10:02
 * @Software: IntelliJ IDEA
 */
public class HbaseUtil {

    /**
     * 获取连接对象，全局，不会关闭连接
     */
    static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    /**
     * 区间查询
     *
     * @param tableName 表名
     * @param family    列族
     * @param colName   列名
     * @param startKey  起始键
     * @param endKey    终止键
     * @return
     */
    public static List<String> scanQuery(String tableName, String family, String colName, String startKey, String endKey) {
        List<String> list = new ArrayList<>();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(startKey.getBytes());
            scan.setStopRow(endKey.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] value = result.getValue(family.getBytes(), colName.getBytes());
                list.add(Arrays.toString(value));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return list;
    }

    /**
     * 测试
     *
     * @param args
     */
    public static void main(String[] args) {
        List<String> list = scanQuery("quot_stock", "info", "data", "00001220201102231059", "00001220201102231739");
        System.out.println(list);
    }


}
