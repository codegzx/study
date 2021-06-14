package com.codegzx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class HDFSTestClass {
    public static void main(String[] args) {

    }

    FileSystem fs;

    @Before
    public void init() {
        System.setProperty("HADOOP_USER_NAME", "root");
        //创建HDFS的java客户端对象
        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata10:9000");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMkdir() {
        boolean b = false;
        try {
            b = fs.mkdirs(new Path("/data/src"));
            b = fs.mkdirs(new Path("/data/output"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(b);
    }

    @Test
    public void testCopyFromLocal() {
        try {
//            fs.copyFromLocalFile(new Path("E:\\resource\\soft_pkg\\Acrobat.9.Pro简体中文免激活版.rar"), new Path("/data/src"));
            fs.copyFromLocalFile(new Path("E:\\resource\\study\\books\\TXT\\庆余年.txt"), new Path("/data/src"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCopyToLocal() {
        try {
            fs.copyToLocalFile(new Path("/data/src/Acrobat.9.Pro简体中文免激活版.rar"), new Path("src/resources/datas/output/Acrobat.9.Pro简体中文免激活版.rar"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //读取文件
    @Test
    public void testRead() {
        try (FSDataInputStream in = fs.open(new Path("/data/src/庆余年.txt"))) {
            byte[] bs = new byte[1024];
            int len = 0;
            while ((len = in.read(bs)) != -1) {
                System.out.println(new String(bs, 0, len));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //写入文件
    @Test
    public void testWrite() {
        try (FSDataOutputStream out = fs.create(new Path("/data/src/test_write.txt"));
             BufferedInputStream in = new BufferedInputStream(
                     new FileInputStream("E:\\resource\\study\\books\\TXT\\庆余年.txt"));) {
            byte[] bs = new byte[1024];
            int len = 0;
            while ((len = in.read(bs)) != -1) {
                out.write(bs, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
