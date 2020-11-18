package me.iroohom.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import me.iroohom.bean.QuotRes;
import me.iroohom.constant.HttpCode;
import me.iroohom.mapper.QuotMapper;
import me.iroohom.service.QuotService;
import me.iroohom.util.DateUtil;
import me.iroohom.util.DbUtil;
import me.iroohom.util.HbaseUtil;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: QuotServiceImpl
 * @Author: Roohom
 * @Function: 业务层 业务逻辑加工
 * @Date: 2020/11/9 10:18
 * @Software: IntelliJ IDEA
 */
@Service
public class QuotServiceImpl implements QuotService {


    @Value("${druid.url}")
    private String url;

    @Value("${druid.driverClassName}")
    private String driverClassName;


    @Autowired
    QuotMapper quotMapper;

    /**
     * 指数数据查询 TODO:待测试
     *
     * @return
     */
    @Override
    public QuotRes indexQuery() throws SQLException {
        //过去Druid连接对象
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();
        String sql = "SELECT indexCode,indexName,preClosePrice,openPrice,closePrice,\n" +
                "round(CAST(closePrice as DOUBLE) - CAST(preClosePrice as DOUBLE),2) as updown,\n" +
                "round((CAST(closePrice as DOUBLE) - CAST(preClosePrice as DOUBLE))/CAST(preClosePrice as DOUBLE),2) as increase,\n" +
                "tradeAmtDay,\n" +
                "tradeVolDay\n" +
                "FROM \"index_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '7' DAY\n" +
                "ORDER BY __time desc \n" +
                "limit 10";

        ResultSet resultSet = statement.executeQuery(sql);
        List<Map<String, Object>> maps = new ArrayList<>();

        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("code", resultSet.getString(1));
            map.put("name", resultSet.getString(2));
            map.put("preClosePrice", resultSet.getString(3));
            map.put("openPrice", resultSet.getString(4));
            map.put("tradePrice", resultSet.getString(5));
            map.put("upDown", resultSet.getString(6));
            map.put("increase", resultSet.getString(7));
            map.put("tradeAmt", resultSet.getString(8));
            map.put("tradeVol", resultSet.getString(9));
            maps.add(map);
        }
        DbUtil.close(resultSet, statement, conn);
        //封装数据返回
        QuotRes quotRes = new QuotRes();
        quotRes.setItems(maps);
        return quotRes;
    }

    /**
     * 板块数据查询 TODO：待测试
     *
     * @return
     */
    @Override
    public QuotRes sectorQuery() throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();
        String sql = "SELECT sectorCode,sectorName,preClosePrice,openPrice,closePrice,tradeAmtDay,tradeVolDay\n" +
                "FROM \"sector_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '7' DAY\n" +
                "ORDER BY __time desc \n" +
                "limit 10";

        ResultSet resultSet = statement.executeQuery(sql);
        List<Map<String, Object>> maps = new ArrayList<>();
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("code", resultSet.getString(1));
            map.put("name", resultSet.getString(1));
            map.put("preClosePrice", resultSet.getString(1));
            map.put("openPrice", resultSet.getString(1));
            map.put("tradePrice", resultSet.getString(1));
            map.put("tradeAmt", resultSet.getString(1));
            map.put("tradeVol", resultSet.getString(1));
            maps.add(map);
        }

        DbUtil.close(resultSet, statement, conn);
        //封装数据返回
        QuotRes quotRes = new QuotRes();
        quotRes.setItems(maps);
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        return quotRes;
    }

    /**
     * 涨幅数据查询 TODO:待测试
     *
     * @return
     */
    @Override
    public QuotRes increaseQuery() throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();

        String sql = "SELECT \n" +
                "secCode,secName,increase,tradePrice,updown,tradeVol,amplitude,preClosePrice,tradeAmt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '10' DAY\n" +
                "ORDER BY __time desc \n" +
                "limit 10";

        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("code", resultSet.getString(1));
            map.put("name", resultSet.getString(2));
            map.put("increase", resultSet.getString(3));
            map.put("tradePrice", resultSet.getString(4));
            map.put("upDown", resultSet.getString(5));
            map.put("tradeVol", resultSet.getString(6));
            map.put("amplitude", resultSet.getString(7));
            map.put("preClosePrice", resultSet.getString(8));
            map.put("tradeAmt", resultSet.getString(9));
            maps.add(map);
        }

        DbUtil.close(resultSet, statement, conn);
        QuotRes quotRes = new QuotRes();
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        quotRes.setItems(maps);

        return quotRes;
    }

    /**
     * 涨停跌停数 TODO:待测试
     *
     * @return
     */
    @Override
    public JSONObject upDownCount() throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();
        //涨停
        String sqlUp = "SELECT tradeTime,count(*) as cnt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '10' DAY\n" +
                "and CAST(increase as DOUBLE) > 0.1\n" +
                "group by 1";

        //跌停
        String sqlDown = "SELECT tradeTime,count(*) as cnt\n" +
                "FROM \"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '10' DAY\n" +
                "and CAST(increase as DOUBLE) <  -0.1\n" +
                "group by 1";


        List<Map<String, Object>> upList = new ArrayList<>();
        List<Map<String, Object>> downList = new ArrayList<>();


        ResultSet resultSet = statement.executeQuery(sqlUp);
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("time", resultSet.getString(1));
            map.put("count", resultSet.getString(2));
            upList.add(map);
        }
        resultSet = statement.executeQuery(sqlDown);
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("time", resultSet.getString(1));
            map.put("count", resultSet.getString(2));
            downList.add(map);
        }
        DbUtil.close(resultSet, statement, conn);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("upList", upList);
        jsonObject.put("downList", downList);
        jsonObject.put("code", HttpCode.SUCC_200.getCode());
        return jsonObject;
    }

    /**
     * 成交量对比
     *
     * @return
     */
    @Override
    public JSONObject compareTradeVol() throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();

        //今日成交量查询
        String curSql = "SELECT tradeTime,sum(PARSE_LONG(tradeVolDay)) as vol\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '7' DAY\n" +
                "group by 1";

        //昨日成交量查询
        String yesSql = "SELECT tradeTime,sum(PARSE_LONG(tradeVolDay)) as vol\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" <= CURRENT_TIMESTAMP - INTERVAL '7' DAY and \"__time\" > CURRENT_TIMESTAMP - INTERVAL '8' DAY\n" +
                "group by 1";
        List<Map<String, Object>> curList = new ArrayList<>();
        List<Map<String, Object>> yesList = new ArrayList<>();

        ResultSet resultSet = statement.executeQuery(curSql);
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("time", resultSet.getString(1));
            map.put("count", resultSet.getString(2));
            curList.add(map);
        }

        resultSet = statement.executeQuery(yesSql);
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("time", resultSet.getString(1));
            map.put("count", resultSet.getString(2));
            yesList.add(map);
        }

        DbUtil.close(resultSet, statement, conn);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("curList", curList);
        jsonObject.put("yesList", yesList);
        jsonObject.put("code", HttpCode.SUCC_200.getCode());

        /**
         * 造数
         */
//        JSONObject jsonObject = new JSONObject();
//        List<JSONObject> list = new ArrayList<>();
//        List<JSONObject> yesList = new ArrayList<>();
//
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm");
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(new java.util.Date());
//        cal.set(Calendar.HOUR_OF_DAY, 9);
//        cal.set(Calendar.MINUTE, 30);
//        long timeInMillis = cal.getTimeInMillis();
//        for (int i = 0; i < 100; i++) {
//            long timeMills = timeInMillis + 60000l * i;
//            String formatDate = simpleDateFormat.format(new Date(timeMills));
//            JSONObject json = new JSONObject();
//            JSONObject yesJson = new JSONObject();
//            json.put("time",formatDate);
//            yesJson.put("time",formatDate);
//            json.put("count",i*10+10);
//            yesJson.put("count",i*10+200);
//            if(i>50){
//                json.put("count",i*10+400);
//                yesJson.put("count",i*10+20);
//            }
//            list.add(json);
//            yesList.add(yesJson);
//        }
//        jsonObject.put("volList",list);
//        jsonObject.put("yesVolList",yesList);
//        return jsonObject;

        return jsonObject;
    }

    /**
     * 个股涨跌幅度查询 TODO:待测试
     *
     * @return
     */
    @Override
    public QuotRes increaseRangeQuery() throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();

        //sql查询
        String sql = "SELECT \n" +
                "\tCASE WHEN CAST(increase AS DOUBLE) >0.07 THEN '>7%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>0.05 AND  CAST(increase AS DOUBLE) <= 0.07 THEN '5~7%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>0.03 AND  CAST(increase AS DOUBLE) <= 0.05 THEN '3~5%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>0 AND  CAST(increase AS DOUBLE) <= 0.03 THEN '0~3%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>-0.03 AND  CAST(increase AS DOUBLE) <= 0 THEN '-3~0%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>-0.05 AND  CAST(increase AS DOUBLE) <= -0.03 THEN '-3~-5%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>-0.07 AND  CAST(increase AS DOUBLE) <= -0.05 THEN '-7~-5%'\n" +
                "\tELSE '-7%' END AS increase,COUNT(*) AS cnt\n" +
                "FROM \n" +
                "\t\"stock_stream_increase\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '7' DAY\n" +
                "GROUP BY \n" +
                "\tCASE WHEN CAST(increase AS DOUBLE) >0.07 THEN '>7%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>0.05 AND  CAST(increase AS DOUBLE) <= 0.07 THEN '5~7%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>0.03 AND  CAST(increase AS DOUBLE) <= 0.05 THEN '3~5%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>0 AND  CAST(increase AS DOUBLE) <= 0.03 THEN '0~3%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>-0.03 AND  CAST(increase AS DOUBLE) <= 0 THEN '-3~0%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>-0.05 AND  CAST(increase AS DOUBLE) <= -0.03 THEN '-3~-5%'\n" +
                "\tWHEN CAST(increase AS DOUBLE)>-0.07 AND  CAST(increase AS DOUBLE) <= -0.05 THEN '-7~-5%'\n" +
                "\tELSE '-7%' END ";

        List<Map<String, Object>> maps = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("title", resultSet.getString(1));
            map.put("count", resultSet.getString(2));
            maps.add(map);
        }

        QuotRes quotRes = new QuotRes();
        quotRes.setItems(maps);
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        return quotRes;
    }


    /**
     * 外盘指数 TODO:待测试
     *
     * @return
     */
    @Override
    public QuotRes externalQuery() {
        List<Map<String, Object>> list = quotMapper.externalQuery();
        QuotRes quotRes = new QuotRes();
        quotRes.setItems(list);
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        return quotRes;
    }

    /**
     * 个股模糊查询
     *
     * @param searchStr 输入字符串参数
     * @return
     */
    @Override
    public QuotRes searchCode(String searchStr) throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();

        String sql = "SELECT DISTINCT  secCode,secName\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '10' DAY\n" +
                "and secCode like '%" + searchStr + "%'\n" +
                "limit 10 ";
        ResultSet rs = statement.executeQuery(sql);
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("code", rs.getString(1));
            map.put("name", rs.getString(2));
            list.add(map);
        }

        //关流
        DbUtil.close(rs, statement, conn);

        //封装结果
        QuotRes quotRes = new QuotRes();
        quotRes.setItems(list);
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        return quotRes;

    }

    /**
     * 个股分时详情数据查询
     *
     * @return
     */
    @Override
    public QuotRes timeSharingQuery(String code) throws SQLException {
        //获取连接对象
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement st = conn.createStatement();

        //查询sql
        String sql = "SELECT secCode,secName,preClosePrice,openPrice,closePrice,highPrice,lowPrice,tradeAmt,tradeVol,tradeVolDay,tradeAmtDay,tradeTime\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '10' DAY\n" +
                "and secCode = '" + code + "'";

        ResultSet rs = st.executeQuery(sql);
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("code", rs.getString(1));
            map.put("name", rs.getString(2));
            map.put("preClosePrice", rs.getString(3));
            map.put("openPrice", rs.getString(4));
            map.put("tradePrice", rs.getString(5));
            map.put("highPrice", rs.getString(6));
            map.put("lowPrice", rs.getString(7));
            map.put("tradeAmt", rs.getString(8));
            map.put("tradeVol", rs.getString(9));
            map.put("tradeVolDay", rs.getString(10));
            map.put("tradeAmtDay", rs.getString(11));
            map.put("date", rs.getString(12));
            list.add(map);
        }

        //关流
        DbUtil.close(rs, st, conn);

        //封装结果
        QuotRes quotRes = new QuotRes();
        quotRes.setItems(list);
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        return quotRes;
    }

    /**
     * 个股日K数据查询
     *
     * @param code
     * @return
     */
    @Override
    public QuotRes dklineQuery(String code) {
        List<Map<String, Object>> list = quotMapper.dklineQuery(code);
        QuotRes quotRes = new QuotRes();
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        quotRes.setItems(list);
        return quotRes;
    }

    /**
     * 个股分时列表
     *
     * @return
     */
    @Override
    public QuotRes stockAll() throws SQLException {
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement statement = conn.createStatement();
        String sql = "SELECT secCode,secName,preClosePrice,openPrice,closePrice,highPrice,lowPrice,tradeAmt,tradeVol,tradeAmtDay,tradeVolDay\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '7' DAY\n" +
                "ORDER BY __time desc \n" +
                "limit 100 ";

        ResultSet resultSet = statement.executeQuery(sql);
        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("code", resultSet.getString(1));
            map.put("name", resultSet.getString(2));
            map.put("preClosePrice", resultSet.getString(3));
            map.put("openPrice", resultSet.getString(4));
            map.put("tradePrice", resultSet.getString(5));
            map.put("highPrice", resultSet.getString(6));
            map.put("lowPrice", resultSet.getString(7));
            map.put("tradeAmt", resultSet.getString(8));
            map.put("tradeVol", resultSet.getString(9));
            map.put("tradeVolDay", resultSet.getString(10));
            map.put("tradeAmtDay", resultSet.getString(11));
            maps.add(map);
        }
        DbUtil.close(resultSet, statement, conn);
        QuotRes quotRes = new QuotRes();
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        quotRes.setItems(maps);

        return quotRes;
    }

    @Override
    public JSONObject stockMinDetail(String code) throws SQLException {
        //获取连接对象
        Connection conn = DbUtil.getConn(driverClassName, url);
        Statement st = conn.createStatement();

        String sql = "SELECT preClosePrice,openPrice,closePrice,highPrice,lowPrice,tradeAmt,tradeVol\n" +
                "FROM \"stock_stream_sse\"\n" +
                "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '10' DAY\n" +
                "and secCode = '" + code + "' \n" +
                "ORDER BY __time desc \n" +
                "limit 1";

        ResultSet rs = st.executeQuery(sql);
        JSONObject jsonObject = new JSONObject();
        while (rs.next()) {
            jsonObject.put("preClosePrice", rs.getString(1));
            jsonObject.put("openPrice", rs.getString(2));
            jsonObject.put("tradePrice", rs.getString(3));
            jsonObject.put("highPrice", rs.getString(4));
            jsonObject.put("lowPrice", rs.getString(5));
            jsonObject.put("tradeAmt", rs.getString(6));
            jsonObject.put("tradeVol", rs.getString(7));
        }

        //关流
        DbUtil.close(rs, st, conn);

        //设置返回码
        jsonObject.put("code", HttpCode.SUCC_200.getCode());
        return jsonObject;
    }

    /**
     * 个股秒级查询 查Hbase TODO:******************
     *
     * @param code
     * @return
     */
    @Override
    public QuotRes stockSecondQuery(String code) {
        /**
         * 开发步骤：
         * 1.获取起止rowkey
         * 2.区间查询
         * 3.获取指定列的数据
         * 4.封装返回数据
         */
        Map<String, String> curSecTime = DateUtil.getCurSecTime();
        //获取当前分钟秒级开始时间戳
        String startSecTime = curSecTime.get("startSecTime");
        //获取当前分钟的秒级结束时间戳
        String endSecTime = curSecTime.get("endSecTime");
        String startKey = code + startSecTime;
        String endKey = code + endSecTime;

        //模拟数据：
        startKey = "00071920201101103800";
        endKey = "00071920201101103859";

        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        List<String> list = HbaseUtil.scanQuery("quot_stock", "info", "data", startKey, endKey);
        for (String str : list) {
            //获取指定列的数据
            JSONObject jsonObject = JSONObject.parseObject(str);
            String tradeTime = jsonObject.getString("tradeTime");
            String closePrice = jsonObject.getString("closePrice");
            String tradeVolDay = jsonObject.getString("tradeVolDay");
            String tradeAmtDay = jsonObject.getString("tradeAmtDay");

            //返回数据封装
            HashMap<String, Object> mapTmp = new HashMap<>();
            mapTmp.put("date", tradeTime);
            mapTmp.put("tradePrice", closePrice);
            mapTmp.put("tradeVol", tradeVolDay);
            mapTmp.put("tradeAmt", tradeAmtDay);
            maps.add(mapTmp);
        }

        //将数据封装返回
        QuotRes quotRes = new QuotRes();
        quotRes.setItems(maps);
        quotRes.setCode(HttpCode.SUCC_200.getCode());
        return quotRes;
    }

    /**
     * 个股主营业务
     *
     * @return
     */
    @Override
    public JSONObject stockDesc(String code) {
        Map<String, Object> map = quotMapper.stockDesc(code);
        //MAP强转换为json
        JSONObject json = (JSONObject) JSON.toJSON(map);
        return json;
    }
}
