package me.iroohom.sse;

import me.iroohom.avro.SseAvro;
import me.iroohom.util.DateTimeUtil;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: SseQuotSourceUser
 * @Author: Roohom
 * @Function: Flume自定义Source数据源
 * @Date: 2020/10/29 10:32
 * @Software: IntelliJ IDEA
 */

public class SseQuotSourceUser extends AbstractSource implements PollableSource, Configurable {
    /**
     * Flume启动之后会自动调用这两个接口的实现类
     */
    private String host;
    private Integer port;
    private String userName;
    private String password;
    private String ftpDirectory;
    private String fileName;
    private String localDirectory;
    private Integer delay;
    private Integer corePoolSize;
    private Integer maxPoolSize;
    private Integer keepAliveTime;
    private Integer capacity;
    ThreadPoolExecutor poolExecutor;

    /**
     * （1）实现目标
     * 使用flume自定义source从FTP服务器采集实时行情文本文件
     * （2）实现步骤
     * 1.实现自定义source接口
     * 2.初始化参数（初始化source参数和线程池）
     * 3.判断是否是交易时间段
     * 4.异步处理
     * 5.设置延时时间
     */

    @Override
    public Status process() throws EventDeliveryException {
        //判断时间是否是9:30-15:00
        long time = System.currentTimeMillis();
        if (time < DateTimeUtil.closeTime && time > DateTimeUtil.openTime) {
            poolExecutor.execute(new AsyncTask());
            //设置延时时间
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //有数据
            return Status.READY;
        }
        //无数据
        return Status.BACKOFF;
    }

    /**
     * 初始化参数
     *
     * @param context 上下文
     */
    @Override
    public void configure(Context context) {
        host = context.getString("host");
        port = context.getInteger("port");
        userName = context.getString("userName");
        password = context.getString("password");
        ftpDirectory = context.getString("ftpDirectory");
        fileName = context.getString("fileName");
        localDirectory = context.getString("localDirectory");
        delay = context.getInteger("delay");

        corePoolSize = context.getInteger("corePoolSize");
        maxPoolSize = context.getInteger("maxPoolSize");
        keepAliveTime = context.getInteger("keepAliveTime");
        capacity = context.getInteger("capacity");

        poolExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue<>(capacity));


    }

    private class AsyncTask implements Runnable {
        /**
         * 开发步骤：
         * 1.创建异步线程task
         * 2.下载行情文件
         * 3.解析并发送数据
         * 数据转换成avro
         * 数据序列化
         * 4.发送数据到channel
         */
        @Override
        public void run() {

            //从FTP下载文件

            try {
                download();
            } catch (IOException e) {
                e.printStackTrace();
            }


            try {
                //解析下载好的本地的FTP文件
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(localDirectory + "/" + fileName))));
                String str;
                int i = 0;
                while ((str = bufferedReader.readLine()) != null) {
                    //如果i=0说明是首行
                    String[] split = str.split("\\|");
                    if (i == 0) {
                        String status = split[8];
                        if (status.startsWith("E")) {
                            break;
                        }
                    } else {
                        //数据转换成avro
                        SseAvro sseAvro = parseAvro(split);
                        //数据序列化
                        byte[] serializer = serializer(sseAvro);
                        getChannelProcessor().processEvent(EventBuilder.withBody(serializer));
                    }
                    i++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        private SseAvro parseAvro(String[] arr) {
            SseAvro sseAvro = new SseAvro();
            sseAvro.setMdStreamID(arr[0].trim());
            sseAvro.setSecurityID(arr[1].trim());
            sseAvro.setSymbol(arr[2].trim());
            sseAvro.setTradeVolume(Long.valueOf(arr[3].trim()));
            sseAvro.setTotalValueTraded(Long.valueOf(arr[4].trim()));
            sseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));
            sseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));
            sseAvro.setHighPrice(Double.valueOf(arr[7].trim()));
            sseAvro.setLowPrice(Double.valueOf(arr[8].trim()));
            sseAvro.setTradePrice(Double.valueOf(arr[9].trim()));
            sseAvro.setClosePx(Double.valueOf(arr[10].trim()));
            sseAvro.setTradingPhaseCode("T11");
            sseAvro.setTimestamp(System.currentTimeMillis());
            return sseAvro;
        }

        private byte[] serializer(SseAvro sseAvro) {
            //定义schema约束
            SpecificDatumWriter<SseAvro> datumWriter = new SpecificDatumWriter<>(SseAvro.class);
            //获取二进制编码对象
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            //注意是directBinaryEncoder
            BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);

            //将avro对象编码成字节输出流
            try {
                datumWriter.write(sseAvro, binaryEncoder);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return bos.toByteArray();
        }

        /**
         * 开发步骤：
         * 1.初始化ftp连接
         * （1）设置IP和Port
         * （2）设置登陆用户名和密码
         * (3) 设置编码格式
         * （4）判断是否连接成功（FTPReply）
         * 2.切换工作目录，设置被动模式
         * 3.获取工作目录的文件信息列表
         * 4.输出文件
         * 5.退出，返回成功状态
         */

        private void download() throws IOException {
            //创建ftpClient连接对象
            FTPClient ftpClient = new FTPClient();
            //初始化ftp连接
            ftpClient.connect(host, port);
            //设置登录名和密码
            ftpClient.login(userName, password);
            //设置编码格式
            ftpClient.setControlEncoding("UTF-8");
            //获取ftp是否成功连接的返回码
            int replyCode = ftpClient.getReplyCode();
            if (FTPReply.isPositiveCompletion(replyCode)) {
                //被动模式，服务器开放端口，用于数据传输
                ftpClient.enterLocalPassiveMode();
                //禁用服务端参与的验证
                ftpClient.setRemoteVerificationEnabled(false);
                //切换工作目录
                ftpClient.changeWorkingDirectory(ftpDirectory);
                //列举所有的文件
                FTPFile[] ftpFiles = ftpClient.listFiles();
                //遍历文件列表
                for (FTPFile ftpFile : ftpFiles) {
                    //输出文件
                    ftpClient.retrieveFile(ftpFile.getName(), new FileOutputStream(new File(localDirectory + "/" + fileName)));
                }
            }
            //退出，返回成功状态
            ftpClient.logout();
        }
    }

}
