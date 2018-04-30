package com.kylin.assembly.spark.streaming;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.Reader;

/**
 * @description: mybatis
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-25
 **/

public class DBTools {
    public static SqlSessionFactory sessionFactory;
    static {
        try {
            Reader reader = Resources.getResourceAsReader("mybatis.cfg.xml");
            sessionFactory = new SqlSessionFactoryBuilder().build(reader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static SqlSession getSession() {
        return sessionFactory.openSession();
    }


}
