package com.edu.bigdata.transform.converter.impl;

import com.edu.bigdata.transform.common.GlobalConstants;
import com.edu.bigdata.transform.converter.IDimensionConverter;
import com.edu.bigdata.transform.dimension.base.BaseDimension;
import com.edu.bigdata.transform.dimension.key.base.BrowserDimension;
import com.edu.bigdata.transform.dimension.key.base.DateDimension;
import com.edu.bigdata.transform.dimension.key.base.KpiDimension;
import com.edu.bigdata.transform.dimension.key.base.PlatformDimension;
import com.edu.bigdata.transform.util.JDBCManager;
import com.edu.bigdata.transform.util.LRUCache;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DimensionConverterImpl implements IDimensionConverter {



    private ThreadLocal<Connection> localConn = new ThreadLocal<>();

    private LRUCache<String, Integer> lruCache = new LRUCache<>(3000);

    public DimensionConverterImpl() {
        // 设置 JVM 关闭时，尝试关闭数据库连接资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> JDBCManager.closeConnection(localConn.get(), null, null)));
    }

    @Override
    public int getDimensionIdByValue(BaseDimension baseDimension) {
        // LRUCache 中缓存数据的格式
        String cacheKey = genCacheKey(baseDimension);

        if (lruCache.containsKey(cacheKey)){
            return lruCache.get(cacheKey);
        }

        String[] sqls = null;
        if (baseDimension instanceof DateDimension){
            sqls = getDateDimensionSQL();
        }else if (baseDimension instanceof BrowserDimension){
            sqls = getBrowserDimensionSQL();
        }else if (baseDimension instanceof KpiDimension){
            sqls = getKpiDimensionSQL();
        }else if (baseDimension instanceof PlatformDimension){
            sqls = getPlatformDimensionSQL();
        }else if (baseDimension instanceof DateDimension){
            sqls = getDateDimensionSQL();
        }else {
            throw new RuntimeException("Cannot match the dimession, unknown dimension.");
        }

        Connection conn = this.getConnection();
        int id = -1;
        synchronized (this){
            id = execSQL(conn,sqls,baseDimension);
        }
        lruCache.put(cacheKey,id);
        return 0;

    }

    private Connection getConnection() {
        Connection conn = null;
        try {
            synchronized (this) {
                conn = localConn.get();
                if (conn == null || conn.isClosed() || !conn.isValid(3)) {
                    Configuration conf = new Configuration();
                    conf.addResource("output-collector.xml");
                    conf.addResource("query-mapping.xml");
                    conf.addResource("transfer-env.xml");
                    try {
                        conn = JDBCManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
                    } catch (SQLException e) {
                        JDBCManager.closeConnection(conn, null, null);
                        conn = JDBCManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
                    }
                }
                this.localConn.set(conn);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return conn;
    }

    private String genCacheKey(BaseDimension baseDimension){
        StringBuilder sb = new StringBuilder();
        if (baseDimension instanceof BrowserDimension){
            BrowserDimension bd = (BrowserDimension) baseDimension;
            sb.append("browser_dimension");
            sb.append(bd.getBrowerName()).append(bd.getBrowerVersion());
        }else if (baseDimension instanceof KpiDimension){
            KpiDimension kd = (KpiDimension) baseDimension;
            sb.append("kpi_dimension");
            sb.append(kd.getKpiName());
        }else if (baseDimension instanceof PlatformDimension){
            PlatformDimension pd = (PlatformDimension) baseDimension;
            sb.append("platform_dimension");
            sb.append(pd.getPlatformName()).append(pd.getPlatformVersion());
        }else if (baseDimension instanceof DateDimension){
            DateDimension dd = (DateDimension) baseDimension;
            sb.append("date_dimension");
            sb.append(dd.getYear()).append(dd.getMonth()).append(dd.getDay());
        }
        if (sb.length() <= 0){
            throw new RuntimeException("can't create cacheKey:"+baseDimension);
        }
        return sb.toString();
    }

    private String[] getBrowserDimensionSQL(){
        String query = "SELECT `id` FROM `tb_dimension_browser` WHERE `platform_name`=? AND `platform_version`=? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_browser` (`platform_name`,`platform_version`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    private String[] getKpiDimensionSQL(){
        String query = "SELECT `id` FROM `tb_dimension_kpi` WHERE `kpi_name`=? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_kpi` (`kpi_name`) VALUES (?);";
        return new String[]{query, insert};
    }

    private String[] getPlatformDimensionSQL(){
        String query = "SELECT `id` FROM `tb_dimension_platform` WHERE `platform_name`=? AND `platform_version`=? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_kpi` (`platform_name`,`platform_version`) VALUES (?,?);";
        return new String[]{query, insert};
    }

    private String[] getDateDimensionSQL(){
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year`=? AND `season`=? AND `month`=? AND `week`=? AND `day`=?  ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`,`season`,`month`,`week`,`day`) VALUES (?,?,?,?,?);";
        return new String[]{query, insert};
    }

    private int execSQL(Connection conn, String[] sqls, BaseDimension baseDimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 1、假设数据库中有该条数据
            // 封装查询的sql语句
            ps = conn.prepareStatement(sqls[0]);
            // 根据不同的维度，封装不同维度的sql查询语句
            setArguments(ps, baseDimension);
            // 执行查询
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1); // 注意：结果集的列的索引从1开始
            }

            // 2、假设数据库中没有该条数据
            // 封装插入的sql语句
            ps = conn.prepareStatement(sqls[1]);
            // 根据不同的维度，封装不同维度的sql插入语句
            setArguments(ps, baseDimension);
            // 执行插入s
            ps.executeUpdate();

            // 3、释放资源
            JDBCManager.closeConnection(conn,ps,rs);

            // 4、此时数据库中有该条数据了，重新获取id，调用自己即可
            // 封装查询的sql语句
            ps = conn.prepareStatement(sqls[0]);
            // 根据不同的维度，封装不同维度的sql查询语句
            setArguments(ps, baseDimension);
            // 执行查询
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1); // 注意：结果集的列的索引从1开始
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            JDBCManager.closeConnection(conn,ps,rs);
        }

        throw new RuntimeException("Failed to get id!");
    }

    private void setArguments(PreparedStatement ps, BaseDimension baseDimension) {
        int i = 0;
        try {
            if (baseDimension instanceof  DateDimension) {
                DateDimension dateDimension = (DateDimension) baseDimension;
                ps.setInt(++i, dateDimension.getYear());
                ps.setInt(++i, dateDimension.getSeason());
                ps.setInt(++i, dateDimension.getMonth());
                ps.setInt(++i, dateDimension.getWeek());
                ps.setInt(++i, dateDimension.getDay());
            }else if (baseDimension instanceof BrowserDimension){
                BrowserDimension bd = (BrowserDimension) baseDimension;
                ps.setString(++i,bd.getBrowerName());
                ps.setString(++i,bd.getBrowerVersion());
            }else if (baseDimension instanceof KpiDimension){
                KpiDimension kd = (KpiDimension) baseDimension;
                ps.setString(++i,kd.getKpiName());
            } else if (baseDimension instanceof PlatformDimension) {
                PlatformDimension pd = (PlatformDimension) baseDimension;
                ps.setString(++i,pd.getPlatformName());
                ps.setString(++i,pd.getPlatformVersion());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
