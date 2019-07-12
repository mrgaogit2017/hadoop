package com.jhh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 *
 * Java 程序对Hadoop的基本操作
 *
 * 创建目录, 添加文件, 读取文件, 追加内容
 * 可参考:http://blog.sina.com.cn/s/blog_4438ac090101pxnj.html
 * @author gaozhaolu
 * @date 2019/6/26 10:42
 */
public class Hadoop {
    public static void main(String[] args) throws IOException {
        // 创建Configuration对象
        Configuration conf = new Configuration();
        // 开启文件追加
        //conf.setBoolean("dfs.support.append", true);
        //conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

        // 创建FileSystem对象
        // 需求：查看hdfs集群服务器/user/zyh/passwd.txt的内容/namelist.txt
        // 必须是active节点,否则不给读取
        URI hdpUri = URI.create("hdfs://172.16.11.208:9000");
        // URI hdpUri = URI.create("hdfs://bi/");
        FileSystem fs = FileSystem.get(hdpUri, conf);

        String newPathStr = "/ble/pp/";
        newPathStr = "/tpp/";

        // 创建新的目录/ble/pp/ 最后的斜杠最好带着
        fs.mkdirs(new Path(newPathStr));

        // 创建文件
        //FSDataOutputStream out = fs.create(new Path(newPathStr));
        //out.write("hello hadoop".getBytes());

        // 上传一个文件, 最后的地址/ble/pp/UAT-JHH-FQ.pem. 如果只写/ble/pp 会认为重命名为pp文件

        //newPathStr = "hdfs://172.16.11.208:9000/tpp/black11.txt";
        fs.copyFromLocalFile(new Path("d:/black11.txt"), new Path(newPathStr+"black11.txt"));

        // 从远程Hadoop下载文件到本地, 需要本地也下载一套hadoop东西
        // 这种需求, 也可以通过读取重新写本地文件实现
        // fs.copyToLocalFile(new Path("/tpp/black.txt"), new Path("d:/black11.txt"));


        /***
         * 文件追加
         *
         * 如果需要开启对于整个HDFS的文件追加内容权限需要在
         * hdfs-site.xml中增加以下配置
         * <property>
         * <name>dfs.support.append</name>
         * <value>true</value>
         * </property>
         *
         * 一般无需配置一下2项, 如果报错在配
         * hdfs-site.xml
         * <property>
         *     <name>dfs.client.block.write.replace-datanode-on-failure.policy</name>
         *     <value>NEVER</value>
         * </property>
         * <property>
         *     <name>dfs.client.block.write.replace-datanode-on-failure.enable</name>
         *     <value>true</value>
         * </property>
         */
        /*try {
            // 通过文件追加
            // File newFile = new File("d:/black.txt");
            // FileInputStream inputStream = new FileInputStream (newFile);

            String netStr = "通过字符串追加的内容: 我是字符串xyz\n";
            ByteArrayInputStream inputStream = new ByteArrayInputStream(netStr.getBytes());

            FSDataOutputStream out = fs.append(new Path("/tpp/black.txt"));
            IOUtils.copyBytes(inputStream, out, 4096, true);
        } catch (Exception e) {
            e.printStackTrace();
        }*/


        // 读取/tpp/black.txt的内容
        String filePathStr = "/tpp/black11.txt";
        FSDataInputStream is = fs.open(new Path(filePathStr));
        byte[] buff = new byte[1024];
        int length;
        while ((length = is.read(buff)) != -1) {
            System.out.println(new String(buff, 0, length));
        }
        is.close();
        fs.close();
        //System.out.println(fs.getClass().getName());
    }
}
