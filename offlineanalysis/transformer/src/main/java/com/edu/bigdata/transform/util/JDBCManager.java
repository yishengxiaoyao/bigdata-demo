package com.edu.bigdata.transform.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.sql.*;

public class JDBCManager {

    public static Connection getConnection(Configuration conf, String warehouse) throws SQLException {
        conf.addResource(new Path(warehouse+"/hive-site.xml"));
        String driverClass = conf.get("javax.jdo.option.ConnectionDriverName");
        String username = conf.get("javax.jdo.option.ConnectionUserName");
        String password = conf.get("javax.jdo.option.ConnectionPassword");
        String url = conf.get("javax.jdo.option.ConnectionURL");
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(url,username,password);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    public static void closeConnection(Connection conn, Statement stat, ResultSet rs) {
        try {
            if (rs != null && !rs.isClosed()){
                rs.close();
            }
            if (stat != null && !stat.isClosed()){
                stat.close();
            }
            if (conn != null && !conn.isClosed()){
                conn.close();
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
