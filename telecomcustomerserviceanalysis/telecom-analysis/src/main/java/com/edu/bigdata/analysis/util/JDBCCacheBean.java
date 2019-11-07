package com.edu.bigdata.analysis.util;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * 单例 JDBC 连接器
 */
public class JDBCCacheBean {

    private static Connection conn = null;

    private JDBCCacheBean() {}

    public static Connection getInstance() {

        try {
            if (conn == null || conn.isClosed() || conn.isValid(3)) {
                conn = JDBCUtil.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }
}