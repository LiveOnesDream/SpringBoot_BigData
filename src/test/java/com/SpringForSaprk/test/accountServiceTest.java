package com.SpringForSaprk.test;

import com.SpringAndSpark.service.impl.AccountServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = "classpath:bean.xml")
public class accountServiceTest {
    private AccountServiceImpl service;

    @Test
    public void test() {
        service.Broadcast_Example();
    }
}
