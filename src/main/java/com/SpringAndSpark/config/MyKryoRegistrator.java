package com.SpringAndSpark.config;

import com.SpringAndSpark.dao.Impl.AccountDaoImpl;
import com.SpringAndSpark.utils.SparkUtils;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * 注册序列化
 */
public class MyKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {

        kryo.register(SparkConfig.class);
        kryo.register(AccountDaoImpl.class);
        kryo.register(SparkUtils.class);
    }
}
