package com.SpringAndSpark.dao.Impl;

import com.SpringAndSpark.config.HBaseConfig;
import com.SpringAndSpark.dao.IHBaseDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;

@Repository("hbasetDao")
public class HBaseDaoImpl implements IHBaseDao {

    private static org.apache.hadoop.conf.Configuration conf = null;
    private static Connection conn = null;

    static {
        System.setProperty("HADOOP_USER_NAME", "root");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "DaFa3,DaFa4,DaFa5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hadoop.user.name", "root");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println("HBase 连接失败");
        }
    }

    @Override
    public void createTbale() {

    }

    @Override
    public void deleteTable(String tableName) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
           if (admin.tableExists(table)){
               admin.disableTable(table);
               admin.deleteTable(table);
               System.out.println(tableName + "is deleted!");
           }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
