package me.roohom.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * hdfs util
 * @author WY
 */
public class HdfsUtil {

    private static final Configuration CONF = new Configuration();

    /**
     * 判断目录是否存在
     * @param path 路径
     * @return 是否存在
     * @throws IOException exception
     */
    public static boolean isExists(String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(CONF);
        return fileSystem.exists(new Path(path));
    }

    /**
     * 合并文件
     * @param path 路径
     * @throws IOException exception
     */
    public static void merge(String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(CONF);

        Path mergePath = new Path(path + "part.merge");
        if (fileSystem.exists(mergePath)) {
            fileSystem.delete(mergePath, false);
        }

        FileStatus[] fileStatus = fileSystem.listStatus(new Path(path));
        List<FileStatus> validFiles = new ArrayList<>();
        boolean hasTmpFile = false;
        for (FileStatus status : fileStatus) {
            if (status.isFile() && !status.getPath().getName().startsWith("_")) {
                validFiles.add(status);
                if (status.getPath().getName().startsWith(".")) {
                    hasTmpFile = true;
                }
            }
        }
        if (!hasTmpFile && validFiles.size() <= 1) {
            return;
        }
        validFiles.sort(Comparator.comparing(FileStatus::getModificationTime));

        fileSystem.createNewFile(mergePath);
        OutputStream out = fileSystem.append(mergePath);

        for (int i = 0; i < validFiles.size(); i++) {
            InputStream in = fileSystem.open(validFiles.get(i).getPath());
            IOUtils.copyBytes(in, out, 4096, false);
            if (i < validFiles.size() - 1) {
                out.write("\n".getBytes());
            }
            in.close();
            if (!validFiles.get(i).getPath().getName().startsWith(".")) {
                fileSystem.rename(validFiles.get(i).getPath(), new Path(path + "." + validFiles.get(i).getPath().getName()));
            }
        }
        out.close();
    }
}
