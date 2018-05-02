package com.kylin.assembly.mysql;

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
// 使用MyBatis提供的Resources类加载mybatis的配置文件
            Reader reader = Resources.getResourceAsReader("mybatis.cfg.xml");
// 构建sqlSession的工厂
            sessionFactory = new SqlSessionFactoryBuilder().build(reader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // 创建能执行映射文件中sql的sqlSession
    public static SqlSession getSession() {
        return sessionFactory.openSession();
    }

    public static void main(String[] args) {
        SqlSession session = DBTools.getSession();
        GwmDealerMapper mapper = session.getMapper(GwmDealerMapper.class);
        GwmDealer gwmDealer = mapper.selectByPrimaryKey(new Integer(983));
        System.out.println(gwmDealer);
        session.commit();

    }
}
