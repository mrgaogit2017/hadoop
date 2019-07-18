package com.jhh.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author gaozhaolu
 * @date 2019/7/11 19:42
 */
public class SparkWordCountWithJava8 {
    public static void main(String[] args) {

        String input = "/tpp/testspark";
        String outPutPath = "/out";
        if (args == null || args.length != 2) {
            System.out.println("参数数量必须为2个");
        } else {
            input = args[0];
            outPutPath = args[1];
        }
        System.out.println("内部设置输入输出路径:");
        System.out.println("InputPath: " + input);
        System.out.println("OutPath: " + outPutPath);

        SparkConf conf = new SparkConf();
        conf.setAppName("WortCount");
        conf.set("spark.submit.deployMode", "cluster");
        conf.setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> fileRDD = sc.textFile(input);
        JavaRDD<String> wordRdd = fileRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator());
        JavaPairRDD<String, Integer> wordOneRDD = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCountRDD = wordOneRDD.reduceByKey((x, y) -> x + y);
        JavaPairRDD<Integer, String> count2WordRDD = wordCountRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        JavaPairRDD<Integer, String> sortRDD = count2WordRDD.sortByKey(false);
        JavaPairRDD<String, Integer> resultRDD = sortRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        resultRDD.saveAsTextFile(outPutPath);

    }
}
