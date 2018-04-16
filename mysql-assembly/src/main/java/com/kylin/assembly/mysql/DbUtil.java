package com.kylin.assembly.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @description: 数据库操作
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-12
 **/
public class DbUtil {

    private static Logger logger = LoggerFactory.getLogger(DbUtil.class);

    public static void execSql(String sql){
        logger.info("prepare exec:{}",sql);
        Connection conn = DbManagerSingleton.getInstance().getConnection();
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            logger.error("执行失败:{}",e.getMessage());
        } finally {
            try {
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}
