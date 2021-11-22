package me.roohom.source;

import jdk.nashorn.internal.runtime.regexp.joni.ast.StringNode;
import me.roohom.utils.HttpUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.util.HashMap;

public class HttpSource extends RichSourceFunction<String> {

    private volatile boolean isRunning = true;
    private String url;
    private long requestInterval;
    private DeserializationSchema<String> deserializer;
    // count out event
    private transient Counter counter;
    private String POST_URL = "http://api.data.csvw.com/auth-center/oauth/token";
    private String GET_URL = "http://api.data.csvw.com/datalake-api/v1.0/audi/getAudiSaleDrivingTestDetail?pageNo=1&pageSize=20";

    public HttpSource(String url, long requestInterval, DeserializationSchema<String> deserializer) {
        this.url = url;
        this.requestInterval = requestInterval;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        counter = new SimpleCounter();
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        HashMap<String, String> postBody = new HashMap<>();
        postBody.put("client_id", "SCB");
        postBody.put("client_secret", "nFbVKGJP");
        postBody.put("grant_type", "client_credentials");


        String tokenJson = HttpUtils.doPost(POST_URL, postBody);
        System.out.println(tokenJson);

        JsonNode jsonNode = objectMapper.readTree(tokenJson);
        JsonNode accessToken = jsonNode.get("access_token");
        JsonNode tokenType = jsonNode.get("token_type");
        String token = tokenType.toString().replace("\"", "") + " " + accessToken.toString().replaceAll("\"", "");
        System.out.println("token:" + token);

        String message = HttpUtils.doGet(GET_URL, token);


        ctx.collect(deserializer.deserialize(message.getBytes()).toString());
        this.counter.inc();

        Thread.sleep(requestInterval);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
