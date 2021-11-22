package me.roohom.parse;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DataParse {
    public static void main(String[] args) throws IOException {
        String json = "{\n" +
                "    \"code\":\"00000000\",\n" +
                "    \"message\":\"success\",\n" +
                "    \"data\":[\n" +
                "        {\n" +
                "            \"vin\":\"LSVCC6F24M2010165\",\n" +
                "            \"factoryplatemodel\":\"SVW73025CK\",\n" +
                "            \"accbmodelname\":\"Audi A7 L CKD\",\n" +
                "            \"accbtypecode\":\"498B2Y\",\n" +
                "            \"invoicekind\":\"机动车发票\",\n" +
                "            \"dealercode\":\"76648019\",\n" +
                "            \"dealername\":\"西安金尚迪汽车销售服务有限公司\",\n" +
                "            \"salesorgname\":\"西区\",\n" +
                "            \"invoicetypr\":\"公司\",\n" +
                "            \"biztype\":\"试乘试驾车\",\n" +
                "            \"discountno\":\"是\",\n" +
                "            \"isvalid\":\"是\",\n" +
                "            \"invoicestatus\":\"已开票\",\n" +
                "            \"invoicedate\":\"2021-10-01\",\n" +
                "            \"totaltaxamount\":\"595710\",\n" +
                "            \"isdelivery\":\"是\",\n" +
                "            \"deliverydate\":null,\n" +
                "            \"salestype\":\"特殊零售\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"vin\":\"LSVCC6F24M2010165\",\n" +
                "            \"factoryplatemodel\":\"SVW73025CK\",\n" +
                "            \"accbmodelname\":\"Audi A7 L CKD\",\n" +
                "            \"accbtypecode\":\"498B2Y\",\n" +
                "            \"invoicekind\":\"机动车发票\",\n" +
                "            \"dealercode\":\"76648019\",\n" +
                "            \"dealername\":\"西安金尚迪汽车销售服务有限公司\",\n" +
                "            \"salesorgname\":\"西区\",\n" +
                "            \"invoicetypr\":\"公司\",\n" +
                "            \"biztype\":\"试乘试驾车\",\n" +
                "            \"discountno\":\"是\",\n" +
                "            \"isvalid\":\"是\",\n" +
                "            \"invoicestatus\":\"已开票\",\n" +
                "            \"invoicedate\":\"2021-10-01\",\n" +
                "            \"totaltaxamount\":\"595710\",\n" +
                "            \"isdelivery\":\"是\",\n" +
                "            \"deliverydate\":null,\n" +
                "            \"salestype\":\"特殊零售\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"queryFlag\":true\n" +
                "}";


        String json1 = "{\"data\":[]}";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNodes = objectMapper.readTree(json);
        JsonNode dataNodes = jsonNodes.get("data");
        List<Object> list = objectMapper.readValue(dataNodes.toString(), List.class);
        //System.out.println(objectMapper.writeValueAsString(list.get(0)));

        list.forEach(
                x -> {
                    try {
                        System.out.println("data:" + objectMapper.writeValueAsString( x));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );

    }
}
