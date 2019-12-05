package com.SpringAndSpark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Repository;

import java.io.Serializable;

@Repository("sparkConfig")
@PropertySource("classpath:sparkConf.properties")
public class SparkConfig implements Serializable {

//    @Value("${spark.appName}")
//    public static String appName;
//    @Value("${spark.master}")
//    private static String master;
//    @Value("${spark.memory}")
//    private static String memory;
//    @Value("${spark.memoryValue}")
//    private static String memoryValue;

    public static SparkConf conf = null;
    public static SparkSession sparkSession = null;
    public static JavaSparkContext sc = null;

    static {
        conf = new SparkConf().setAppName("SpringForSpark").setMaster("local[2]")
                .set("spark.testing.memory", "2147480000");
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
//                        registerKryoClasses(new Class[]{SprarkConfig.class, AccountDaoImpl.class});
        sparkSession = SparkSession.builder().config(conf)
                .config("spark.KryoSerializer", "config.MyKryoRegistrator").getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());
    }

}
