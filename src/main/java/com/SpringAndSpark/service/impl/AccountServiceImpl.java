package com.SpringAndSpark.service.impl;

import com.SpringAndSpark.dao.IAccountDao;
import com.SpringAndSpark.dao.Impl.AccountDaoImpl;
import com.SpringAndSpark.service.IAccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 业务层实现类
 */
@Service("accountService")
public class AccountServiceImpl implements  IAccountService {

    @Autowired
    private IAccountDao accounDao;

    @Override
    public void queryAccount() {
        accounDao.queryAccount();
    }

    @Override
    public void Broadcast_Example()  {
        accounDao.Broadcast_Example();
    }

    @Override
    public void readFile() {
        accounDao.readFile();
    }


}
