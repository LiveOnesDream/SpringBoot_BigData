package com.SpringAndSpark.dao.Impl;

import com.SpringAndSpark.config.SparkConfig;
import com.SpringAndSpark.dao.IAccountDao;
import com.SpringAndSpark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 持久层实现类
 */
@Repository("accountDao")
public class AccountDaoImpl implements IAccountDao, Serializable {

    @Autowired
    private SparkConfig config;

    @Autowired
    private SparkUtils utils;

    @Override
    public void queryAccount() {
        utils.getMysqlTbale();
    }

    @Override
    public void Broadcast_Example() {
        List<String> list = Arrays.asList(
                "001,刘向前,18,0",
                "002,冯  剑,28,1",
                "003,李志杰,38,0",
                "004,郭  鹏,48,2");

        Map<String, String> map = new HashMap<String, String>();
        map.put("0", "女");
        map.put("1", "男");

        final Broadcast<Map<String, String>> broadcast = config.sc.broadcast(map);
        JavaRDD<String> userRDD = config.sc.parallelize(list);
        JavaRDD<String> result = userRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String prefix = s.substring(0, s.indexOf(","));
                String gender = s.substring(s.lastIndexOf(",") + 1);
                Map<String, String> map1 = broadcast.value();
                String newGender = map1.getOrDefault(gender, "男");

                return prefix + ":" + newGender;
            }
        });
        result.foreach(s -> System.out.println(s));
    }

    @Override
    public void readFile() {
        JavaRDD<String> lines = config.sc.textFile("C:\\Users\\24490\\Desktop\\testData\\aaa.txt");
        lines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }


}
