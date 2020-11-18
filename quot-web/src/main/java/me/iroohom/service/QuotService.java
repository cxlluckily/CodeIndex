package me.iroohom.service;

import com.alibaba.fastjson.JSONObject;
import me.iroohom.bean.QuotRes;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

/**
 * @ClassName: QuotService
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/9 10:18
 * @Software: IntelliJ IDEA
 */

public interface QuotService {
    public QuotRes indexQuery() throws SQLException;

    QuotRes sectorQuery() throws SQLException;

    QuotRes increaseQuery() throws SQLException;

    JSONObject upDownCount() throws SQLException;

    JSONObject compareTradeVol() throws SQLException;

    QuotRes increaseRangeQuery() throws SQLException;

    QuotRes externalQuery();

    QuotRes searchCode(String searchStr) throws SQLException;

    QuotRes timeSharingQuery(String code) throws SQLException;

    QuotRes dklineQuery(String code);

    QuotRes stockAll() throws SQLException;

    JSONObject stockMinDetail(String code) throws SQLException;

    QuotRes stockSecondQuery(String code);

    JSONObject stockDesc(String code);

}
