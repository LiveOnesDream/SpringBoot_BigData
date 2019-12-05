package com.SpringAndSpark.utils;

import com.SpringAndSpark.config.SparkConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.util.Properties;

@Repository("sparkUtils")
@PropertySource("classpath:sparkConf.properties")
public class SparkUtils implements Serializable {

    @Value("${mysql.url}")
    private String url;
    @Value("${mysql.table}")
    private String table;
    @Value("${mysql.username}")
    private String user;
    @Value("${mysql.password}")
    private String password;
    @Value("${mysql.driver}")
    private String driver;

    @Autowired
    private SparkConfig config;

    public void getMysqlTbale() {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        properties.put("driver", driver);

        config.sparkSession.read().jdbc(url, table, properties).show();
    }
}
