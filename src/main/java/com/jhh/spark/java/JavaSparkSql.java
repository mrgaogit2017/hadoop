package com.jhh.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * SparkSql
 * 测试未通过...
 * 一般样例给的是Scala或java的单机模式
 * @author gaozhaolu
 * @date 2019/7/18 19:16
 */
public class JavaSparkSql {
    public static void main(String[] args) {
        /*SparkSession spark = SparkSession.builder()
                .appName("Spark Sql")
                //yarn-cluster|yarn-client
                //.master("spark://172.16.11.201:7077")
                .master(args[0])
                //.config("spark.submit.deployMode", "cluster")
                .config("spark.authenticate", "true")
                .config("spark.authenticate.secret", "123456")
                // 内存
                .config("spark.testing.memory", "2147480000")
                .config("spark.driver.allowMultipleContexts", "true")
                .getOrCreate();
        Dataset<Row> ds = spark.read().json("/tmp/tr.json");
        ds.show();*/

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark Sql")
                .setSparkHome("/apps/dev/spark-2.4.3-bin-hadoop2.7")
                .set("spark.authenticate.secret", "123456")
                .set("spark.authenticate", "true")
                .set("spark.testing.memory", "2147480000")
                .setMaster(args[0]);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> ds = sqlContext.read().json("/tmp/tr.json");
        ds.show();

        System.out.println("初始化完毕");
        //Dataset<String> ds1 = spark.read().textFile("");
    }
}
