package me.roohom.mbb.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.audi.tss.avro.callback.DataBlock;
import de.audi.tss.avro.callback.Field;
import de.audi.tss.avro.callback.VehicleDataMetaInfo;
import de.audi.tss.avro.callback.VehicleDataReport;
import de.audi.tss.avro.feedback.Feedback;
import de.audi.tss.avro.feedback.feedbackMessageEnum;
import me.roohom.mbb.env.FlinkEnv;
import me.roohom.mbb.sink.KafkaPro;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;

public class SendDataApp {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment streamEnv = FlinkEnv.getStreamEnv();
        //DataStreamSource<String> vehStreamSource = streamEnv.readTextFile("data/mbb/received_data.json");

        ObjectMapper objectMapper = new ObjectMapper();

        //File file = new File("data/mbb/received_data.json");
        File file = new File("data/mbb/feedback.json");
        String jsonString = txt2String(file);
        System.out.println(jsonString);

        //VehicleDataReport vehicleDataReport = objectMapper.readValue(jsonString, VehicleDataReport.class);
        //System.out.println(vehicleDataReport.toString());

        Feedback feedback = objectMapper.readValue(jsonString, Feedback.class);
        System.out.println(feedback.toString());

        KafkaPro<SpecificRecordBase> kafkaPro = new KafkaPro<>();

        //VehicleDataMetaInfo vehicleDataMetaInfo = vehicleDataReport.getVehicleDataMetaInfo();
        //String vin = vehicleDataMetaInfo.getVin().toString();
        int i = 0;
        while (i < 100000) {
            //vehicleDataMetaInfo.setVin(vin + String.valueOf(i));
            //if (i % 3 == 0) {
            //    vehicleDataMetaInfo.setBrand("VW");
            //}
            //if (i % 3 == 1) {
            //    vehicleDataMetaInfo.setBrand("SKD");
            //}
            //if (i % 3 == 2) {
            //    vehicleDataMetaInfo.setBrand("AUDI");
            //}
            //for (DataBlock dataBlock : vehicleDataReport.getVehicleDataList()) {
            //    for (Field field : dataBlock.getFieldList()) {
            //        field.setMilCarCaptured(field.getMilCarCaptured() + Math.abs(new Random(100).nextInt()));
            //        field.setMilCarSent(field.getMilCarSent() + Math.abs(new Random(1000).nextInt()));
            //        field.setTsCarCaptured("2022-01-05");
            //    }
            //}
            feedback.setCorrelationId(feedback.getCorrelationId() + String.valueOf(i));
            if (i % 5 ==0)
            {
                feedback.setFeedbackMessage(feedbackMessageEnum.NACK_INVALID_MESSAGE);
            }
            if (i % 5 ==1)
            {
                feedback.setFeedbackMessage(feedbackMessageEnum.ACK_SUBSCRIPTION_SUCCESSFUL);
            }
            if (i % 5 ==2)
            {
                feedback.setFeedbackMessage(feedbackMessageEnum.ACK_UNSUBSCRIPTION_SUCCESSFUL);
            }
            if (i % 5 ==3)
            {
                feedback.setFeedbackMessage(feedbackMessageEnum.NACK_NO_PERMISSION);
            }
            if (i % 5 ==4)
            {
                feedback.setFeedbackMessage(feedbackMessageEnum.NACK_UNKNOWN_ERROR);
            }


            //kafkaPro.sendData("senddata_from_tss", vehicleDataReport);
            kafkaPro.sendData("feedback_from_tss", feedback);
            //System.out.println(vehicleDataReport.toString());
            i++;
        }


    }

    public static String txt2String(File file) {
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                result.append(System.lineSeparator() + s);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}
