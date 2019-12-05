package com.SpringAndSpark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HDFSUtils {

    final static String InPutPath = "C:\\Users\\24490\\Desktop\\hbase-site.xml";
    final static String outPutPath = "/hbase-site.xml";

    /**
     * 写数据到HDFS
     *
     * @param readPath
     * @param writeFileName
     */
    public static void writeData(String readPath, String writeFileName) {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fileSystem = null;
        FileInputStream fis = null;
        FSDataOutputStream fos = null;
        try {
            fileSystem = FileSystem.get(conf);
            Path path = new Path(writeFileName);
            fos = fileSystem.create(path);
            fis = new FileInputStream(new File(readPath));
            IOUtils.copyBytes(fis, fos, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    public static void main(String[] args) {
        // System.setProperty("HADOOP_USER_NAME","hdfs");
        // System.setProperty("HADOOP_USER_PASSWORD","hdfs");
        writeData(InPutPath,outPutPath);
    }
}
