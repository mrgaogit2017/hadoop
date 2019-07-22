import com.jhh.spark.java.InputStreamReaderRunnable;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;

/**
 *
 * 使用Java调用Spark By SparkLauncher方式
 *
 * @author gaozhaolu
 * @date 2019/7/12 19:29
 */
public class LauncherApp {

    /**
     * 支持集群方式 --  亲测通过
     * @param input
     * @param outPutPath
     */
    public static void SparkLauncherTest(String input, String outPutPath) {
        HashMap env = new HashMap<>(2);
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR", "/apps/dev/hadoop-2.7.7/etc/hadoop");
        env.put("JAVA_HOME", "/usr/local/java/jdk1.8.0_211");
        //env.put("YARN_CONF_DIR","");

        SparkLauncher handle = new SparkLauncher(env)
                .setAppName("Test Java-Spark By SparkLauncher")
                .setSparkHome("/apps/dev/spark-2.4.3-bin-hadoop2.7")
                // 需要单独另外一个计算jar 也可以写HDFS路径的jar
                .setAppResource("/apps/jars/spk-jar-1.0-sql.jar")
                //.setAppResource("/apps/jars/spk-jar-1.0-SNAPSHOT.jar")
                // 计算jar的主类
                .setMainClass("JavaSparkSql")
                //.setMainClass("SparkDemo")
                .addAppArgs(new String[]{input, outPutPath})
                //spark://mini02:7077
                .setMaster("yarn")
                .setDeployMode("cluster")
                /*.setConf("spark.app.id", "11222")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.akka.frameSize", "200")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.instances", "32")
                .setConf("spark.executor.cores", "3")
                .setConf("spark.default.parallelism", "10")*/
                .setConf("spark.driver.allowMultipleContexts", "true")
                .setVerbose(true);
        //.startApplication(new SparkAppHandle.Listener(){...}) // 这种方式需要实现监听接口的方法, 不用写下面的Stream输出了
        try {
            Process process = handle.launch();

            // 监控过程中的输入 -- 非必须
            InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
            Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
            inputThread.start();

            // 监控过程中的错误信息 -- 非必须
            InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
            Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
            errorThread.start();

            System.out.println("Waiting for finish...");
            int exitCode = process.waitFor();
            System.out.println("Finished! Exit code:" + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /***
     * Spark Standalone 模式才行-- 未测
     *
     * 需要一个REST URL:
     * "--master", "spark://192.168.0.181:6066"
     *
     * @param input
     * @param outPutPath
     */
    public static void SparkSubmitTest(String input, String outPutPath) {
        String[] args = {
                "--class", "com.jhh.spark.java.SparkWordCountWithJava8",
                "--master", "spark://mini02:7077",
                "--deploy-mode", "cluster",
                "--executor-memory", "200m",
                "--total-executor-cores", "2",
                "/apps/jars/spk-jar-1.0-SNAPSHOT.jar",
                "/tpp/testspark",
                "/outSubmit"
        };

        SparkSubmit.main(args);
    }


    public static void main(String[] args) {
        System.out.println("LauncherApp...");

        String input = "/tpp/testspark";
        String outPutPath = "/out_idea";
        if (args != null && args.length == 2) {
            input = args[0];
            outPutPath = args[1];
        }
        System.out.println("输入:" + input);
        System.out.println("输出:" + outPutPath);

        // 1. SparkLauncher方式
        SparkLauncherTest(input, outPutPath);

        // 2. 脚本直接提交方式
        // SparkSubmitTest(input, outPutPath);

    }
}
