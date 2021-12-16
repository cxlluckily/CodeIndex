package me.roohom.jdbc;

import me.roohom.utils.DbUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class JdbcSinkConnector extends RichSinkFunction<Row> {


    //创建构造方法
    private String sql;

    public JdbcSinkConnector(String sql) {
        this.sql = sql;
    }

    Connection conn = null;
    PreparedStatement pst = null;

    @Override
    public void open(Configuration parameters) throws Exception {

        //获取mysql连接
        conn = DbUtil.getConnByJdbc();
        pst = conn.prepareStatement(sql);
        //开启事务
        conn.setAutoCommit(false);

    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        int arity = value.getArity(); //字段长度
        for (int i = 0; i < arity; i++) {
            pst.setObject(i + 1, value.getField(i));
        }
        pst.executeUpdate();
        //提交事务
        conn.commit();
    }

    @Override
    public void close() throws Exception {
        if (pst != null) {
            pst.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
