package start;

import kafka.KafkaPro;

import java.io.*;

/**
 * @ClassName: WriteDataToKafka
 * @Author: Roohom
 * @Function:
 * @Date: 2021/1/28 21:37
 * @Software: IntelliJ IDEA
 */
public class WriteDataToKafka {
    public static void main(String[] args) throws IOException, InterruptedException {
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\VolksWagen\\BUG\\mos-gvs-mysql-vehicle_tm_vehicle_series.txt")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\rooho\\Desktop\\mos-gvs-mysql-vehicle_tm_vehicle_series.txt")));
        String str;
        int i = 0;

        KafkaPro kafkaPro = new KafkaPro();
        while ((str = reader.readLine()) != null) {
            String[] strings = str.split("\r\n");
            System.out.println(strings[0].toString());
            i += 1;
            System.out.println(i);
            Thread.sleep(200);
            kafkaPro.sendData("cdp.fox.tm_vehicle_series",strings[0]);
        }

    }
}
