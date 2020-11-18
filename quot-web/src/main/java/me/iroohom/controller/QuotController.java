package me.iroohom.controller;

import com.alibaba.fastjson.JSONObject;
import me.iroohom.bean.QuotRes;
import me.iroohom.service.QuotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

/**
 * @ClassName: QuotController
 * @Author: Roohom
 * @Function: 控制层 开发请求接口 代码越少越好
 * @Date: 2020/11/9 10:16
 * @Software: IntelliJ IDEA
 */

@RestController //包含Controller和Respondbody，表示返回数据以Json数据返回
@RequestMapping("quot")
public class QuotController {

    @Autowired
    QuotService quotService;

    /**
     * 1、指数查询
     *
     * @return
     */
    @RequestMapping("index/all")
    public QuotRes indexQuery() throws SQLException {
        return quotService.indexQuery();
    }

    /**
     * 2、板块查询
     *
     * @return
     */
    @RequestMapping("sector/all")
    public QuotRes sectorQuery() throws SQLException {
        return quotService.sectorQuery();
    }


    /**
     * 3、个股涨幅榜查询
     *
     * @return
     */
    @RequestMapping("stock/increase")
    public QuotRes increaseQuery() throws SQLException {
        return quotService.increaseQuery();
    }

    /**
     * 4、涨停跌停数
     *
     * @return
     */
    @RequestMapping("stock/updown/count")
    public JSONObject upDownCount() throws SQLException {
        return quotService.upDownCount();
    }

    /**
     * 5、成交量对比
     *
     * @return
     */
    @RequestMapping("stock/tradevol")
    public JSONObject compareTradeVol() throws SQLException {
        return quotService.compareTradeVol();
    }

    /**
     * 6、个股涨跌幅查询
     *
     * @return
     */
    @RequestMapping("stock/updown")
    public QuotRes increaseRangeQuery() throws SQLException {
        return quotService.increaseRangeQuery();
    }


    /**
     * 7、外盘指数查询 查询MySQL
     *
     * @return
     */
    @RequestMapping("external/idnex")
    public QuotRes externalQuery() {
        return quotService.externalQuery();
    }

    /**
     * 8、个股分时列表查询
     *
     * @return
     */
    @RequestMapping("stock/all")
    public QuotRes stockAll() throws SQLException {
        return quotService.stockAll();
    }


    /**
     * 9、个股模糊查询
     *
     * @param searchStr
     * @return
     */
    @RequestMapping("stock/search")
    public QuotRes searchCode(String searchStr) throws SQLException {
        return quotService.searchCode(searchStr);
    }


    /**
     * 10、个股分时详情数据查询
     *
     * @param code
     * @return
     */
    @RequestMapping("stock/screen/time-sharing")
    public QuotRes timeSharingQuery(String code) throws SQLException {
        return quotService.timeSharingQuery(code);
    }


    /**
     * 11、个股日K数据查询
     *
     * @param code 传入参数 个股代码
     * @return
     */
    @RequestMapping("stock/screen/dkline")
    public QuotRes dklineQuery(String code) {
        return quotService.dklineQuery(code);
    }


    /**
     * 12、个股分时详情 指定个股代码下的最新一条
     *
     * @return
     */
    @RequestMapping("stock/screen/second/detail")
    public JSONObject stockMinDetail(String code) throws SQLException {
        return quotService.stockMinDetail(code);
    }

    /**
     * 13、秒级数据查询
     *
     * @param code 个股代码
     * @return
     */
    @RequestMapping("stock/screen/second")
    public QuotRes stockSecondQuery(String code) {
        return quotService.stockSecondQuery(code);
    }

    /**
     * 14、个股主营业务描述
     *
     * @param code 股票代码
     * @return
     */
    @RequestMapping("stock/describe")
    public JSONObject stockDesc(String code) {
        return quotService.stockDesc(code);
    }


}
