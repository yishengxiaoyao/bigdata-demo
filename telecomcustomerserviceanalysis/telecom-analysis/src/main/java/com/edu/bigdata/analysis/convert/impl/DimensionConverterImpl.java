package com.edu.bigdata.analysis.convert.impl;

import com.edu.bigdata.analysis.convert.DimensionConverter;
import com.edu.bigdata.analysis.kv.base.BaseDimension;
import com.edu.bigdata.analysis.kv.key.ContactDimension;
import com.edu.bigdata.analysis.kv.key.DateDimension;
import com.edu.bigdata.analysis.util.JDBCCacheBean;
import com.edu.bigdata.analysis.util.JDBCUtil;
import com.edu.bigdata.analysis.util.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DimensionConverterImpl implements DimensionConverter {

    // 日志记录类，注意导包的正确性
    private static final Logger looger = LoggerFactory.getLogger(DimensionConverterImpl.class); // 打印 DimensionConverterImpl 的日志

    // 为每个线程保留自己的 Connection 实例（JDBC连接器）
    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    // 创建数据缓存队列
    private LRUCache<String, Integer> lruCache = new LRUCache<>(3000);

    public DimensionConverterImpl() {
        looger.info("stopping mysql connection ...");

        // 设置 JVM 关闭时，尝试关闭数据库连接资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> JDBCUtil.close(threadLocalConnection.get(), null, null)));

        looger.info("mysql connection is successful closed");
    }

    /**
     * 根据传入的维度对象，得到该维度对象对应的在表中的主键id（如果数据量特别大，需要用到缓存）
     * 1、内存缓存，LRUCache
     *      1.1 缓存中有数据：直接返回id
     *      1.2 缓存中没有数据：
     *          1.1.1 查询Mysql
     *              1.1.1.1 Mysql中有该条数据，直接返回id，将本次读取到的id缓存到内存中
     *              1.1.1.2 Mysql中没有该数据，插入该条数据，插入成功后，再次反查该数据，得到id并返回，缓存到内存中
     *
     * @param baseDimension
     * @return
     */
    @Override
    public int getDimensionId(BaseDimension baseDimension) {
        // LRUCache 中缓存数据的格式
        // 时间维度：date_dimension_year_month_day,10
        // 查询人维度：contact_dimension_telphone_name,12

        // 1、根据传入的维度对象取得该维度对象对应的 cacheKey
        String cackeKey = genCacheKey(baseDimension);

        // 2、判断缓存中是否存在该 cacheKey 缓存，有数据就直接返回id
        if (lruCache.containsKey(cackeKey)) {
            return lruCache.get(cackeKey);
        }

        // 3、缓存中没有，就去查询数据库，执行 select 操作
        // sqls 中包含了一组sql语句：分别是查询和插入
        String[] sqls = null;
        if (baseDimension instanceof DateDimension) {
            // 时间维度表 tb_dimension_date
            sqls = genDateDimensionSQL();
        } else if (baseDimension instanceof ContactDimension) {
            // 查询人维度表 tb_dimension_contacts
            sqls = genContactDimensionSQL();
        } else {
            // 抛出 Checked 异常，提醒调用者可以自行处理。
            throw new RuntimeException("Cannot match the dimession, unknown dimension.");
        }

        // 4、准备对 MySQL 中的表进行操作，先查询，有可能再插入
        Connection conn = this.getConnection();
        int id = -1;
        synchronized (this) {
            id = execSQL(conn, sqls, baseDimension);
        }

        // 将查询到的id缓存到内存中
        lruCache.put(cackeKey, id);

        return id;
    }

    /**
     * 尝试获取数据库连接对象：先从线程缓冲中获取，没有可用连接则创建新的单例连接器对象。
     *
     * @return
     */
    private Connection getConnection() {
        Connection conn = null;
        try {
            conn = threadLocalConnection.get();
            if (conn == null || conn.isClosed() || conn.isValid(3)) {
                conn = JDBCCacheBean.getInstance();
            }
            threadLocalConnection.set(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 执行 SQL 语句
     *
     * @param conn  JDBC 连接器
     * @param sqls  长度为2，第一个为查询语句，第二个为插入语句
     * @param baseDimension 对应维度所保存的数据
     * @return
     */
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
            JDBCUtil.close(null, ps, rs);

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
            JDBCUtil.close(null, ps, rs);
        }

        throw new RuntimeException("Failed to get id!");
    }

    /**
     * 根据不同的维度，封装不同维度的sql语句
     *
     * @param ps
     * @param baseDimension
     */
    private void setArguments(PreparedStatement ps, BaseDimension baseDimension) {
        int i = 0;
        try {
            if (baseDimension instanceof  DateDimension) {
                DateDimension dateDimension = (DateDimension) baseDimension;
                ps.setInt(++i, dateDimension.getYear());
                ps.setInt(++i, dateDimension.getMonth());
                ps.setInt(++i, dateDimension.getDay());
            } else if (baseDimension instanceof  ContactDimension) {
                ContactDimension contactDimension = (ContactDimension) baseDimension;
                ps.setString(++i, contactDimension.getTelephone());
                ps.setString(++i, contactDimension.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 生成查询人维度表的数据库查询语句和插入语句
     *
     * @return
     */
    private String[] genContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_contacts` WHERE `telephone`=? AND `name`=? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 生成时间维度表的数据库查询语句和插入语句
     *
     * @return
     */
    private String[] genDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year`=? AND `month`=? AND `day`=? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 根据传入的维度对象取得该维度对象对应的 cacheKey
     * LRUCACHE 中缓存的键值对形式例如：<date_dimension20170820, 3> 或者 <contact_dimension15837312345张三, 12>
     *
     * @param baseDimension
     * @return
     */
    private String genCacheKey(BaseDimension baseDimension) {

        StringBuilder sb = new StringBuilder();

        if (baseDimension instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) baseDimension;
            // 拼装缓存 id 对应的 key
            sb.append("date_dimension");
            sb.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        } else if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            // 拼装缓存 id 对应的 key
            sb.append("contact_dimension");
            sb.append(contactDimension.getTelephone()).append(contactDimension.getName());
        }

        if (sb.length() <= 0) {
            throw new RuntimeException("Cannot create cacheKey." + baseDimension);
        }

        return sb.toString();
    }
}