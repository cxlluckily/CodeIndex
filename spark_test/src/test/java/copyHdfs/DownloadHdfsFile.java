package copyHdfs;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用java API操作hdfs--拷贝部分文件到本地
 */
public class DownloadHdfsFile {
    private static final Log log = LogFactory.getLog(DownloadHdfsFile.class);

    public static void main(String[] args) throws IOException {
        DownloadHdfsFile d = new DownloadHdfsFile();
        ByteArrayOutputStream zos = (ByteArrayOutputStream) d.down2("hdfs://svldl067.csvw.com:8020/ci_sc/dws_tt_invoice_info/dt=20210623");
        //byte[] out = zos.toByteArray();
        FileOutputStream f = new FileOutputStream("/Users/roohom/data/xxx.zip");
        f.write(zos.toByteArray());
        zos.close();
    }

    /**
     * 多文件(文件夹)
     *
     * @param cloudPath cloudPath
     * @return 执行结果
     * @author liudz
     * @date 2020/6/8
     **/
    public OutputStream down2(String cloudPath) {
        // 1获取对象
        ByteArrayOutputStream out = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://10.79.169.24:8020"), conf);
            out = new ByteArrayOutputStream();
            ZipOutputStream zos = new ZipOutputStream(out);
            compress(cloudPath, zos, fs);
            zos.close();
        } catch (IOException e) {
            log.info("----error:{}----" + e.getMessage());
        } catch (URISyntaxException e) {
            log.info("----error:{}----" + e.getMessage());
        }
        return out;
    }

    /**
     * compress
     *
     * @param baseDir         baseDir
     * @param zipOutputStream zipOutputStream
     * @param fs              fs
     * @author liudz
     * @date 2020/6/8
     **/
    public void compress(String baseDir, ZipOutputStream zipOutputStream, FileSystem fs) throws IOException {

        try {
            FileStatus[] fileStatulist = fs.listStatus(new Path(baseDir));
            log.info("basedir = " + baseDir);
            String[] strs = baseDir.split("/");
            //lastName代表路径最后的单词
            String lastName = strs[strs.length - 1];

            for (int i = 0; i < fileStatulist.length; i++) {

                String name = fileStatulist[i].getPath().toString();
                name = name.substring(name.indexOf("/" + lastName));

                if (fileStatulist[i].isFile()) {
                    Path path = fileStatulist[i].getPath();
                    FSDataInputStream inputStream = fs.open(path);
                    zipOutputStream.putNextEntry(new ZipEntry(name.substring(1)));
                    IOUtils.copyBytes(inputStream, zipOutputStream, Integer.parseInt("1024"));
                    inputStream.close();
                } else {
                    zipOutputStream.putNextEntry(new ZipEntry(fileStatulist[i].getPath().getName() + "/"));
                    log.info("fileStatulist[i].getPath().toString() = " + fileStatulist[i].getPath().toString());
                    compress(fileStatulist[i].getPath().toString(), zipOutputStream, fs);
                }
            }
        } catch (IOException e) {
            log.info("----error:{}----" + e.getMessage());
        }
    }

}
