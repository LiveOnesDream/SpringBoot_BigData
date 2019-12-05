package com.SpringAndSpark.utils;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.text.DecimalFormat;

//参考文献：https://blog.csdn.net/BD_AI_IoT/article/details/78396898
//         https://blog.csdn.net/login_sonata/article/details/53440059
//         https://blog.csdn.net/u010670689/article/details/33737989
public class imageUtils {

    static BASE64Encoder encoder = new sun.misc.BASE64Encoder();
    static BASE64Decoder decoder = new sun.misc.BASE64Decoder();
    static final String imgInputPath = "C:\\Users\\24490\\Desktop\\test\\";
    static final String imgOutPath = "C:\\Users\\24490\\Desktop\\test\\";

    public static void getImageAttribute() {
        int GB = 1024 * 1024 * 1024;//定义GB的计算常量
        int MB = 1024 * 1024;//定义MB的计算常量
        int KB = 1024;//定义KB的计算常量

        try {
            File file = new File(imgInputPath);
            File[] listFiles = file.listFiles();
            for (File f : listFiles) {
                String name = f.getName();
                String imageValue = getImageBinary(f.toString());//获取图片数据

                System.out.println(imageValue.getBytes("GB2312").length);
                DecimalFormat df = new DecimalFormat("0.00");
                String resultSize = df.format(imageValue.getBytes("GB2312").length / (float) KB) + "kb";
                System.out.println(resultSize);

                System.out.println(name);
                System.out.println(imageValue);

                base64StringToImage(imageValue, imgOutPath);

            }
        } catch (UnsupportedEncodingException e) {
            System.out.println();
        }
    }

    // 将图片转成二进制文件
    public static String getImageBinary(String address) {
        File f = new File(address);
        BufferedImage bi;
        try {
            bi = ImageIO.read(f);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bi, "png", baos); //经测试转换的图片是格式这里就什么格式，否则会失真  
            byte[] bytes = baos.toByteArray();
            return encoder.encodeBuffer(bytes).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //获取图片（需要转换二进制文件）并保存
    public static void base64StringToImage(String base64String, String address) {
        try {
            byte[] bytes1 = decoder.decodeBuffer(base64String);

            ByteArrayInputStream bais = new ByteArrayInputStream(bytes1);
            BufferedImage bi1 = ImageIO.read(bais);

            File w2 = new File(address + "//" + "查找文件" + ".jpg");// 可以是jpg,png,gif格式  
            ImageIO.write(bi1, "jpg", w2);// 不管输出什么格式图片，此处不需改动  
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        getImageAttribute();
    }
}
