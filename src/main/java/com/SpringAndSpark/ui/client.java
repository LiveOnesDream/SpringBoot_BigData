package com.SpringAndSpark.ui;

import com.SpringAndSpark.service.IAccountService;
import com.SpringAndSpark.service.IHBaseService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class client {
    public static void main(String[] args) {

        ApplicationContext ac = new ClassPathXmlApplicationContext("bean.xml");
        IAccountService as = (IAccountService) ac.getBean("accountService");
        IHBaseService hs = (IHBaseService) ac.getBean("hbaseService");
        // as.Broadcast_Example();
        // as.queryAccount();
        // as.readFile();
        hs.deleteTable("student");


//        AccountServiceImpl service = new AccountServiceImpl();
//        service.Broadcast_Example();
    }
}
