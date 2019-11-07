package com.edu.bigdata.analysis.util;

import java.sql.*;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/db_telecom?userUnicode=true&characterEncoding=UTF-8";

    private static final String MYSQL_USERNAME = "root";

    private static final String MYSQL_PASSWORD = "123456";


    /**
     * 实例化 JDBC 连接器对象
     *
     * @return
     */
    public static Connection getConnection() {

        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            return DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 关闭数据库连接器释放资源
     *
     * @param conn
     * @param stat
     * @param rs
     */
    public static void close(Connection conn, Statement stat, ResultSet rs) {

        try {
            if (rs != null && !rs.isClosed()) {
                rs.close();
            }

            if (stat != null && !stat.isClosed()) {
                stat.close();
            }

            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}