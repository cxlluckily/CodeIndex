package me.iroohom.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: QueryUnit
 * @Author: Roohom
 * @Function: 接收数据的pojo
 * @Date: 2020/10/13 18:35
 * @Software: IntelliJ IDEA
 */
@Data
public class QueryUnit {
    private String address;
//    private Account account;
//    private List<Account> accounts;

    private Map<String,Account> accountMap;
}

