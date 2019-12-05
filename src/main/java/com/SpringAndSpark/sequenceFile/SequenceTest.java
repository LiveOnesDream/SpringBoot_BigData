package com.SpringAndSpark.sequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class SequenceTest {

    public static final String LOCAL_SOURCE_PATH = "C:\\Users\\24490\\Desktop\\video\\";
    public static final String LOCAL_PATH = "C:\\Users\\24490\\Desktop\\video1\\";
    public static final String HDFS_PATH = "/sequenceVideoOutPut/";
    private static Configuration conf = null;
    private static FileSystem fs = null;

    static {
        conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
    }

    @SuppressWarnings("deprecation")
    public static void readSmallVideoFile(String pathStr) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathStr);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        FileOutputStream fos = null;
        int i = 0;
        while (reader.next(key, value)) {
            System.out.println(key);
            File file = new File(LOCAL_PATH + key + ".mp4");
            byte[] bs = value.getBytes();
            fos = new FileOutputStream(file);
            fos.write(bs);
        }
        IOUtils.closeStream(reader);
    }

    /**
     * 合并小文件 测试
     */
    @SuppressWarnings("deprecation")
    public static void MergeSmallVideoFile(String inPutPath, String outPath) throws IOException {
        fs = FileSystem.get(conf);
        Path path = new Path(outPath);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class,
                BytesWritable.class, SequenceFile.CompressionType.BLOCK);
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        FileInputStream fis = null;
        File files = new File(inPutPath);
        File[] listFiles = files.listFiles();
        for (File file : listFiles) {
            String video = file.getPath();
            String videoName = file.getName();
            fis = new FileInputStream(video);
            byte[] videoValue = new byte[fis.available()];
            fis.read(videoValue);//将文件内容写入字节数组
            key.set(videoName);
            value.set(videoValue, 0, videoValue.length);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }

    public static void main(String[] args) throws IOException {

        readSmallVideoFile(HDFS_PATH);
        // MergeSmallVideoFile(LOCAL_SOURCE_PATH, HDFS_PATH);
    }
}

