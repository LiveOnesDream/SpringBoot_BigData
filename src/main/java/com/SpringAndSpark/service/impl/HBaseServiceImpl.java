package com.SpringAndSpark.service.impl;

import com.SpringAndSpark.dao.Impl.HBaseDaoImpl;
import com.SpringAndSpark.service.IHBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("hbaseService")
public class HBaseServiceImpl implements IHBaseService {

    @Autowired
    HBaseDaoImpl hbaseDao;
    @Override
    public void createTbale() {

    }

    @Override
    public void deleteTable(String tableName) {
        hbaseDao.deleteTable(tableName);
    }
}
