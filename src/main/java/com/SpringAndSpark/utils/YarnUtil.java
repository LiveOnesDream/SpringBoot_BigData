package com.SpringAndSpark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

//https://blog.csdn.net/aA518189/article/details/88570211

//hdfs dfs -rm -r /output
//hadoop jar  /opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/jars/hadoop-examples.jar wordcount /input/word.txt /output/
public class YarnUtil {

    public static EnumSet<YarnApplicationState> appStates = null;
    public static EnumSet<YarnApplicationState> finishedStates = null;
    public static List<ApplicationReport> appsReport = null;
    public static FinalApplicationStatus status = null;
    public static YarnClient client = null;

    static {
        client = YarnClient.createYarnClient();
        Configuration conf = new Configuration();
        client.init(conf);
        client.start();
    }

    /**
     * 获取RunningApp信息
     */
    public static ApplicationId getRunningApp() {
        appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
        }
        // for (YarnApplicationState state : appStates) {
        //     if (!state.equals("RUNNING")) {
        //         System.exit(0);
        //     }
        // }
        try {
            //返回EnumSet<YarnApplicationState>中个人任务是running状态的任务
            appsReport = client.getApplications(appStates);
            ApplicationId id = null;
            for (ApplicationReport appReport : appsReport) {
                id = appReport.getApplicationId();
                System.out.println("任务ID：" + appReport.getApplicationId().toString()
                        + "  任务名称：" + appReport.getName() + "  程序类型：" + appReport.getApplicationType());
                System.out.println("集群使用情况：" + appReport.getApplicationResourceUsageReport());
                status = appReport.getFinalApplicationStatus();
                System.out.println("最终状态：" + status);
                System.out.println("任务执行百分比：" + appReport.getProgress());
            }
            return id;
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        } finally {
            closeClient();
        }
        return null;
    }

    public static void getSatae() {
        try {
            ApplicationReport report = client.getApplicationReport(getRunningApp());
            System.out.println(report.getProgress());
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取完成的APP信息
     */
    public static void getFinishedApp() {
        finishedStates = EnumSet.noneOf(YarnApplicationState.class);
        if (finishedStates.isEmpty()) {
            finishedStates.add(YarnApplicationState.FINISHED);
        }
        //返回EnumSet<YarnApplicationState>中个人任务rfinished状态的任务
        try {
            appsReport = client.getApplications(finishedStates);
            for (ApplicationReport report : appsReport) {
                System.out.println("任务ID：" + report.getApplicationId().toString()
                        + "  任务名称：" + report.getName() + "  程序类型：" + report.getApplicationType());
                System.out.println("集群使用情况：" + report.getApplicationResourceUsageReport());
                status = report.getFinalApplicationStatus();
                System.out.println("最终状态：" + status);
            }
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        } finally {
            closeClient();
        }
    }

    /**
     * 关闭客户端
     */
    public static void closeClient() {
        try {
            if (client == null) {
                client.close();
            }
        } catch (IOException e) {
            System.out.println("stop client filed");
        }
    }

    /**
     * 间隔执行
     *
     * @param time
     */
    public static void intervalPerform(int time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int i = 0;
        while (true) {
            try {
                Thread.sleep(time * 1000); //设置暂停的时间 秒
                System.out.println(sdf.format(new Date()) + "第" + (i++) + "次获取");
                getRunningApp();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

    }
}
