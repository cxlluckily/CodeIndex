package me.iroohom;

import java.sql.*;

/**
 * @ClassName: KylinTest
 * @Author: Roohom
 * @Function: 测试使用JDBC链接Kylin
 * @Date: 2020/11/8 16:38
 * @Software: IntelliJ IDEA
 */
public class KylinTest {
    /**
     * 开发步骤：
     * 1.加载驱动
     * 2.建立连接
     * 3.创建statement对象
     * 4.写sql，执行sql查询
     * 5.获取查询结果
     * 6.关流
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.kylin.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:kylin://node01:7070/itcast_quot_dm", "ADMIN", "KYLIN");

        String sql = "select \n" +
                "sec_code,\n" +
                "trade_date,\n" +
                "max(high_price) as high_price,\n" +
                "min(low_price) as low_price,\n" +
                "min(trade_vol_day) as min_vol_day,\n" +
                "max(trade_vol_day) as max_vol_day,\n" +
                "min(trade_amt_day) as min_amt_day,\n" +
                "max(trade_amt_day) as max_amt_day\n" +
                "from app_sec_quot_stock\n" +
                "where trade_date between '2020-11-01' and '2020-11-07'\n" +
                "group by 1,2";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);


        while (resultSet.next()) {
            System.out.println("secCode:" + resultSet.getString(1) + ",trade_date:" + resultSet.getString(2));
        }


        if (resultSet != null) {
            resultSet.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}
