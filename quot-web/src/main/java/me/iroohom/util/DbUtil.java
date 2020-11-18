package me.iroohom.util;

import java.sql.*;

/**
 * @ClassName: DbUtil
 * @Author: Roohom
 * @Function: 通过数据库JDBC获取连接，供Druid使用
 * @Date: 2020/11/9 10:11
 * @Software: IntelliJ IDEA
 */
public class DbUtil {
    /**
     * 获取连接
     *
     * @param driverName 驱动类名
     * @param url        连接地址
     * @return
     */
    public static Connection getConn(String driverName, String url) {
        Connection connection = null;
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(url);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关流
     *
     * @param result     结果集
     * @param st         预处理对象
     * @param connection 数据库连接
     * @throws SQLException
     */
    public static void close(ResultSet result, Statement st, Connection connection) throws SQLException {
        try {
            if (result != null) {
                result.close();
            }
            if (st != null) {
                st.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
