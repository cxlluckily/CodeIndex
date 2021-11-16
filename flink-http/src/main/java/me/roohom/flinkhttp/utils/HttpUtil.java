package me.roohom.flinkhttp.utils;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpUtil {


    private static final Logger log = LoggerFactory.getLogger(HttpUtil.class);

    /**
     * 默认超时时间
     */
    private static final int DEFAULT_TIME_OUT = 3000;


    //public static String doGet(String httpurl) throws IOException {
    //    HttpURLConnection connection = null;
    //    InputStream is = null;
    //    BufferedReader br = null;
    //    // 返回结果字符串
    //    String result = null;
    //    try {
    //        // 创建远程url连接对象
    //        URL url = new URL(httpurl);
    //        // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
    //        connection = (HttpURLConnection) url.openConnection();
    //        // 设置连接方式：get
    //        connection.setRequestMethod("GET");
    //        // 设置连接主机服务器的超时时间：15000毫秒
    //        connection.setConnectTimeout(15000);
    //        // 设置读取远程返回的数据时间：60000毫秒
    //        connection.setReadTimeout(60000);
    //        // 发送请求
    //        connection.connect();
    //        // 通过connection连接，获取输入流
    //        if (connection.getResponseCode() == 200) {
    //            is = connection.getInputStream();
    //            // 封装输入流is，并指定字符集
    //            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    //
    //            // 存放数据
    //            StringBuffer sbf = new StringBuffer();
    //            String temp = null;
    //            while ((temp = br.readLine()) != null) {
    //                sbf.append(temp);
    //                sbf.append("\r\n");
    //            }
    //            result = sbf.toString();
    //        }
    //    } catch (MalformedURLException e) {
    //        e.printStackTrace();
    //    } catch (IOException e) {
    //        e.printStackTrace();
    //    } finally {
    //        // 关闭资源
    //        if (null != br) {
    //            try {
    //                br.close();
    //            } catch (IOException e) {
    //                e.printStackTrace();
    //            }
    //        }
    //        if (null != is) {
    //            try {
    //                is.close();
    //            } catch (IOException e) {
    //                e.printStackTrace();
    //            }
    //        }
    //        connection.disconnect();
    //    }
    //    return result;
    //}

    /**
     * get请求，超时时间默认
     * @param api 请求URL
     * @return 响应JSON字符串
     */
    public static String doGet(String api) {
        return doGet(api, DEFAULT_TIME_OUT);
    }


    /**
     * get请求，超时时间传参
     * @param api 请求URL
     * @param timeOut 请求超时时间（毫秒）
     * @return 响应JSON字符串
     */
    public static String doGet(String api, int timeOut) {
        HttpGet httpGet = new HttpGet(api);
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeOut)
                .setConnectionRequestTimeout(timeOut)
                .build();
        httpGet.setConfig(config);

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpGet)) {
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            log.error("get " + api + " failed!", e);
        }
        return null;
    }

    /**
     * post请求，超时时间默认
     * @param api 请求URL
     * @param body 请求体JSON字符串
     * @return 响应JSON字符串
     */
    public static String doPost(String api, String body) {
        return doPost(api, body, DEFAULT_TIME_OUT);
    }


    /**
     * post请求，超时时间传参
     * @param api 请求URL
     * @param body 请求体JSON字符串
     * @param timeOut 请求超时时间（毫秒）
     * @return 响应JSON字符串
     */
    public static String doPost(String api, String body, int timeOut) {
        HttpPost httpPost = new HttpPost(api);
        StringEntity entity = new StringEntity(body, "utf-8");
        entity.setContentType("application/json");
        entity.setContentEncoding("utf-8");
        httpPost.setEntity(entity);
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeOut)
                .setConnectionRequestTimeout(timeOut)
                .build();
        httpPost.setConfig(config);

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpPost)) {
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            log.error("post " + api + " failed!", e);
        }
        return null;
    }



}
