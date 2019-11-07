package com.edu.bigdata.analysis.outputformat;


import com.edu.bigdata.analysis.convert.impl.DimensionConverterImpl;
import com.edu.bigdata.analysis.kv.base.BaseDimension;
import com.edu.bigdata.analysis.kv.base.BaseValue;
import com.edu.bigdata.analysis.kv.key.ComDimension;
import com.edu.bigdata.analysis.kv.value.CountDurationValue;
import com.edu.bigdata.analysis.util.Constants;
import com.edu.bigdata.analysis.util.JDBCCacheBean;
import com.edu.bigdata.analysis.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLOutputFormat extends OutputFormat<BaseDimension, BaseValue> {

    private OutputCommitter committer = null;

    @Override
    public RecordWriter<BaseDimension, BaseValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 初始化JDBC连接器对象
        Connection conn = null;
        try {
            conn = JDBCCacheBean.getInstance();
            // 关闭自动提交，以便于批量提交
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IOException(e);
        }

        return new MysqlRecordWriter(conn);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // 校验输出
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 根据源码怎么实现的模仿写
        if (committer == null) {
            //文件的输出位置
            String name = taskAttemptContext.getConfiguration().get("");
            Path outputPath = name == null ? null : new Path(name);
            committer = new FileOutputCommitter(outputPath, taskAttemptContext);
        }
        return committer;
    }

    static class MysqlRecordWriter extends RecordWriter<BaseDimension, BaseValue> {

        private Connection conn = null;

        private DimensionConverterImpl dci = null;

        private PreparedStatement ps = null;

        private String insertSQL = null;

        private int count = 0;

        private int batchNumber = 0;

        public MysqlRecordWriter(Connection conn) {
            this.conn = conn;
            this.dci = new DimensionConverterImpl();
            this.batchNumber = Constants.JDBC_DEFAULT_BATCH_NUMBER;
        }

        @Override
        public void write(BaseDimension key, BaseValue value) throws IOException, InterruptedException {
            try {
                // 向Mysql中tb_call表写入数据
                // tb_call：id_contact_date, id_dimension_contact, id_dimension_date, call_sum, call_duration_sum

                // 封装SQL语句
                if (insertSQL == null) {
                    insertSQL = "INSERT INTO `tb_call` (`id_contact_date`, `id_dimension_contact`, `id_dimension_date`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_contact_date`=?;";
                }

                // 执行插入操作
                if (ps == null) {
                    ps = conn.prepareStatement(insertSQL);
                }


                ComDimension comDimension = (ComDimension) key;
                CountDurationValue countDurationValue = (CountDurationValue) value;

                // 封装要写入的数据
                int id_dimension_contact = dci.getDimensionId(comDimension.getContactDimension());
                int id_dimension_date = dci.getDimensionId(comDimension.getDateDimension());

                String id_contact_date = id_dimension_contact + "_" + id_dimension_date;

                int call_sum = countDurationValue.getCallSum();
                int call_duration_sum = countDurationValue.getCallDurationSum();

                // 本次SQL
                int i = 0;
                ps.setString(++i, id_contact_date);
                ps.setInt(++i, id_dimension_contact);
                ps.setInt(++i, id_dimension_date);
                ps.setInt(++i, call_sum);
                ps.setInt(++i, call_duration_sum);

                // 有则插入，无则更新的判断依据
                ps.setString(++i, id_contact_date);

                ps.addBatch();

                // 当前缓存了多少个sql语句，等待批量执行，计数器
                count++;
                if (count >= this.batchNumber) {
                    // 批量插入
                    ps.executeBatch();
                    // 连接提交
                    conn.commit();
                    count = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                if (ps != null) {
                    ps.executeBatch();
                    this.conn.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                JDBCUtil.close(conn, ps, null);
            }
        }
    }
}