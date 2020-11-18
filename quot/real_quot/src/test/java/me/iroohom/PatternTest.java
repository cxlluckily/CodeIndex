package me.iroohom;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: PatternTest
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/4 10:19
 * @Software: IntelliJ IDEA
 */
public class PatternTest {
    public static void main(String[] args) {
        String str = "adsncxed132";

        //数字0-9
        Pattern pattern = Pattern.compile("\\d+");
        //规则匹配数据
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            System.out.println("有数字");
        } else {
            System.out.println("无数字");
        }
    }
}
