package com.SpringAndSpark.utils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class hbaseTest {

    static Logger logger = LoggerFactory.getLogger(hbaseTest.class);
    static org.apache.hadoop.conf.Configuration conf = null;
    static Connection conn = null;

    public static Connection getConn() {
        try {
            conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void createTable(String tableName) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = getConn().getAdmin();
            if (!admin.tableExists(table)) {
                HTableDescriptor descriptor = new HTableDescriptor(table);
                descriptor.addFamily(new HColumnDescriptor("cf".getBytes()));
                admin.createTable(descriptor);
            } else {
                System.out.println(tableName + "  table Exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        createTable("student");
    }
}
