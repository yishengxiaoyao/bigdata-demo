package com.edu.spark.high;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HDFSApp {
    Configuration configuration = null;
    URI uri = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        uri = new URI("hdfs://localhost:8020");
        fileSystem = FileSystem.get(uri, configuration);
    }

    @Test
    public void test01() throws Exception {
        fileSystem.mkdirs(new Path("xiaoyao"));
    }

    @Test
    public void test02() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/xiaoyao/test/xiaoyao.txt"));
        out.writeUTF("xiaoyao");
        out.writeUTF("\r\n");
        out.writeUTF("yishengxiaoyao");
        out.flush();
        out.close();
    }

    @Test
    public void test03() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/xiaoyao/test/xiaoyao.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
    }

    @Test
    public void test04() throws Exception {
        fileSystem.rename(new Path("/xiaoyao/test/xiaoyao.txt"), new Path("/xiaoyao/test/yishengxiaoyao.txt"));
    }

    @Test
    public void test05() throws Exception {
        fileSystem.copyFromLocalFile(new Path(""), new Path(""));
        //是否删除本地文件
        fileSystem.copyToLocalFile(new Path(""), new Path(""));
        fileSystem.listFiles(new Path(""), false);
    }
}
