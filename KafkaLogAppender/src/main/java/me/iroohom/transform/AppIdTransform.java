package me.iroohom.transform;

/**
 * @ClassName: AppIdTransform
 * @Author: Roohom
 * @Function: 根据YARN的container_id转换得到application_id
 * @Date: 2021/3/28 21:45
 * @Software: IntelliJ IDEA
 */
public class AppIdTransform {
    public static String getAppId(String containerId) {
        String appId = "";
        String[] strings = containerId.split("_");
        appId = "application_" + strings[1] + "_" + strings[2];

        return appId;
    }
}
