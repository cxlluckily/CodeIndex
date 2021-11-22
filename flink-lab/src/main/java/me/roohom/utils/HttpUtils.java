package me.roohom.utils;

import lombok.SneakyThrows;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class HttpUtils {


    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    /**
     * 默认超时时间
     */
    private static final int DEFAULT_TIME_OUT = 3000;

    /**
     * get请求，超时时间默认
     *
     * @param api 请求URL
     * @return 响应JSON字符串
     */
    public static String doGet(String api, String token) {
        return doGet(api, token, DEFAULT_TIME_OUT);
    }


    /**
     * get请求，超时时间传参
     *
     * @param api     请求URL
     * @param timeOut 请求超时时间（毫秒）
     * @return 响应JSON字符串
     */
    public static String doGet(String api, String token, int timeOut) {
        HttpGet httpGet = new HttpGet(api);
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeOut)
                .setConnectionRequestTimeout(timeOut)
                .setAuthenticationEnabled(true)
                .build();

        httpGet.setConfig(config);
        httpGet.addHeader(new BasicHeader("Content-Type", "application/json"));
        httpGet.addHeader(new BasicHeader("Authorization", token));

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
     *
     * @param api  请求URL
     * @param body 请求体JSON字符串
     * @return 响应JSON字符串
     */
    public static String doPost(String api, HashMap<String, String> body) {
        return doPost(api, body, DEFAULT_TIME_OUT);
    }


    /**
     * post请求，超时时间传参
     *
     * @param api     请求URL
     * @param body    请求体JSON字符串
     * @param timeOut 请求超时时间（毫秒）
     * @return 响应JSON字符串
     */
    @SneakyThrows
    public static String doPost(String api, HashMap<String, String> body, int timeOut) {
        HttpPost httpPost = new HttpPost(api);

        ArrayList<NameValuePair> pairs = new ArrayList<>();
        body.forEach(
                (x, y) -> pairs.add(new BasicNameValuePair(x, y))
        );

        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
        httpPost.setEntity(new UrlEncodedFormEntity(pairs, "UTF-8"));
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