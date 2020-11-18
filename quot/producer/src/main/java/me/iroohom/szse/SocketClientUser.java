package me.iroohom.szse;

import me.iroohom.avro.SzseAvro;
import me.iroohom.kafka.KafkaPro;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @ClassName: SocketClientUser
 * @Author: Roohom
 * @Function: 数据采集客户端
 * @Date: 2020/10/28 16:08
 * @Software: IntelliJ IDEA
 * <p>
 * 开发步骤:
 * 1.创建main方法
 * 2.建立socket连接，获取流数据
 * 3.读文件缓存成交量和成交金额
 * 4.解析行数据，数据转换
 * 5.发送kafka
 */
public class SocketClientUser {


    /**
     * 随机浮动成交价格系数
     */
    private static Double[] price = new Double[]{0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, -0.1, -0.11, -0.12, -0.13, -0.14, -0.15, -0.16, -0.17, -0.18, -0.19, -0.2};

    /**
     * 随机浮动成交量
     */
    private static int[] volume = new int[]{50, 80, 110, 140, 170, 200, 230, 260, 290, 320, 350, 380, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300};

    /**
     * 用于缓存的hashmap，目的是为了后面获取当前的最低价和最高价
     */
    public static Map<String, Map<String, Long>> map = new HashMap<>();

    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("localhost", 4444);
        //读取数据
        InputStream inputStream = socket.getInputStream();
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        KafkaPro kafkaPro = new KafkaPro();

        while (true) {
            //读取每一条数据
            String s = dataInputStream.readUTF();
            //解析数据，数据转换
            SzseAvro szseAvro = parseStr(s);
            System.out.println(szseAvro);

            kafkaPro.sendData("szse", szseAvro);


        }

    }

    /**
     * 成员方法，不是类，不需要class,解析socket中的每一条字符串数据<br/>
     * 开发步骤：<br/>
     * 1.拷贝成交量和成交价数组<br/>
     * 2.获取随机浮动成交量和价格<br/>
     * 3.字符串切割、计算最新价<br/>
     * 4.获取缓存的成交量/金额<br/>
     * 5.计算总成交量/金额<br/>
     * 6.缓存总成交量/金额<br/>
     * 7.获取最高价和最低价(和最新价比较)<br/>
     * 8.封装结果数据<br/>
     *
     * @param str socket流中的数据
     * @return SzseAvro数据
     */
    public static SzseAvro parseStr(String str) {

        Random random = new Random();
        //随机价格系数
        int pIndex = random.nextInt(price.length);
        //随机波动成交量
        int vIndex = random.nextInt(volume.length);

        //随机价格系数和波动成交量
        Double randomPrice = price[pIndex];
        int randomVolume = volume[vIndex];


        //字符串切割，获取最新价格
        String[] split = str.split("\\|");
        //产品代码
        String productCode = split[1].trim();

        //为提高精度，使用bigDecimal
        BigDecimal tradePrice = new BigDecimal(split[9].trim());
        //获取浮动最新价,并设置精度为2位小数
        tradePrice = tradePrice.multiply(new BigDecimal(1 + randomPrice)).setScale(2, RoundingMode.HALF_UP);

        //解析获取成交量
        Long tradeVol = Long.valueOf(split[3].trim());
        //解析获取成交金额
        long tradeAmt = Double.valueOf(split[4].trim()).longValue();

        Long totalVol = 0L;
        Long totalAmt = 0L;

        Map<String, Long> amtVolMap = map.get(productCode);

        if (amtVolMap == null) {
            //如果是第一次，缓存里没有该productCode的数据，amtVolMap即为空,表示没有数据，则就将总交易额和交易总量设置为当前交易额和交易量
            totalAmt = tradeAmt;
            totalVol = tradeVol;

            //将数据缓存
            HashMap<String, Long> cacheMap = new HashMap<>();

            cacheMap.put("tradeAmt", totalAmt);
            cacheMap.put("tradeVol", totalVol);
            map.put(productCode, cacheMap);
        } else {
            //如果获取到的amtVolMap不为空，说明获取到了数据，不是第一次的数据，已经有了交易额和交易量
            //先获取当前总交易量和总交易额
            Long tradeAmtTmp = amtVolMap.get("tradeAmt");
            Long tradeVolTmp = amtVolMap.get("tradeVol");

            //当前的总交易量等于总交易量加波动交易量(新增的交易量)
            totalVol = tradeVolTmp + randomVolume;

            //增量的成交金额 = 成交价格 * 浮动成交量
            BigDecimal tmpAmt = tradePrice.multiply(new BigDecimal(randomVolume));
            //总的成交金额 = 当前成交金额 + 增量的成交金额
            totalAmt = tradeAmtTmp + tmpAmt.longValue();

            //将数据缓存
            HashMap<String, Long> cacheMap = new HashMap<>();

            cacheMap.put("tradeAmt", totalAmt);
            cacheMap.put("tradeVol", totalVol);
            map.put(productCode, cacheMap);

        }

        //获取最高价
        BigDecimal highPrice = new BigDecimal(split[7].trim());
        if (tradePrice.compareTo(highPrice) > 0) {
            highPrice = tradePrice;
        }

        //获取最低价
        BigDecimal lowPrice = new BigDecimal(split[8].trim());
        if (tradePrice.compareTo(lowPrice) < 0) {
            lowPrice = tradePrice;
        }

        SzseAvro szseAvro = new SzseAvro();
        szseAvro.setMdStreamID(split[0].trim());
        szseAvro.setSecurityID(productCode);
        szseAvro.setSymbol(split[2].trim());
        szseAvro.setTradeVolume(totalVol);
        szseAvro.setTotalValueTraded(totalAmt);
        szseAvro.setPreClosePx(new Double(split[5].trim()));
        szseAvro.setOpenPrice(new Double(split[6].trim()));
        szseAvro.setHighPrice(highPrice.doubleValue());
        szseAvro.setLowPrice(lowPrice.doubleValue());
        szseAvro.setTradePrice(tradePrice.doubleValue());
        //在15点之前，收盘价和最新价数据一样
        szseAvro.setClosePx(tradePrice.doubleValue());
        szseAvro.setTradingPhaseCode("T11");
        szseAvro.setTimestamp(System.currentTimeMillis());
        return szseAvro;
    }

}
