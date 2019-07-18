package com.jhh.spark.java;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author gaozhaolu
 * @date 2019/7/12 19:29
 */
public class LauncherApp11 {
    public static void main(String[] args) throws IOException, InterruptedException {

        HashMap env = new HashMap<>();
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR","/tmp/software/hadoop-2.7.7/etc/hadoop");
        env.put("JAVA_HOME","/usr/local/java/jdk1.8.0_211");
        //env.put("YARN_CONF_DIR","");

        SparkLauncher handle = new SparkLauncher(env)
                .setSparkHome("/tmp/software/spark-2.4.3-bin-hadoop2.7")
                .setAppResource("/tmp/hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .setMainClass("com.jhh.spark.java.SparkWordCountWithJava8")
                .setMaster("yarn")
                .setDeployMode("cluster")
                /*.setConf("spark.app.id", "11222")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.akka.frameSize", "200")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.instances", "32")
                .setConf("spark.executor.cores", "3")
                .setConf("spark.default.parallelism", "10")*/
                .setConf("spark.driver.allowMultipleContexts","true")
                .setVerbose(true);


        Process process =handle.launch();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);

    }
}
