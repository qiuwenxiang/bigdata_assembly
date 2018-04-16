package com.kylin.assembly.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.kylin.assembly.common.GlobalParamValue;
import com.kylin.assembly.common.constant.MysqlConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * @description: db库单例管理线程池
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-11
 **/
public class DbManagerSingleton {

    private static Logger logger = LoggerFactory.getLogger(DbManagerSingleton.class);

    private static class SingletonHolder {
        private static final DbManagerSingleton INSTANCE = new DbManagerSingleton();
    }

    private DbManagerSingleton (){}

    public static final DbManagerSingleton getInstance() {
        return SingletonHolder.INSTANCE;
    }
    private static DruidDataSource cpds = new DruidDataSource();

    private static Map<String,String> map = GlobalParamValue.paramMap;

    static {
        cpds.setUrl(map.get(MysqlConstant.DATASOURCE_URL));
        cpds.setDriverClassName(map.get(MysqlConstant.DATASOURCE_DRIVERCLASS));
        cpds.setUsername(map.get(MysqlConstant.DATASOURCE_USERNAME));
        cpds.setPassword(map.get(MysqlConstant.DATASOURCE_PASSWORD));
        cpds.setInitialSize(Integer.parseInt(map.get(MysqlConstant.DATASOURCE_INITIALSIZE)));
        cpds.setMaxActive(Integer.parseInt(map.get(MysqlConstant.DATASOURCE_MAXACTIVE)));
    }

    public Connection getConnection(){
        try {
            return cpds.getConnection();
        } catch (SQLException e) {
            logger.error("get connect error:{}"+e.getMessage());
            return null;
        }
    }



    public static void main(String[] args) {
        DbManagerSingleton.getInstance().getConnection();
    }

}
