package me.iroohom.mapper;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * DAO层 直接操作数据库
 * @author roohom
 */

@Mapper
@Component
public interface QuotMapper {

    public List<Map<String, Object>> query();

    public List<Map<String, Object>> externalQuery();

    List<Map<String, Object>> dklineQuery(@Param("code") String code);

    Map<String, Object> stockDesc(@Param("code") String code);

    Map<String, Object> queryTccDate();

    List<Map<String, Object>> queryKline(@Param("tableName") String tableName, @Param("firstTxdate") String firstTxdate, @Param("tradeDate") String tradeDate);


    void updateKline(@Param("tableName") String tableName, @Param("firstTxdate") String firstTxdate, @Param("tradeDate") String tradeDate);
}



