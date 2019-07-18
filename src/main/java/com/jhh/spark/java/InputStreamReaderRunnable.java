package com.jhh.spark.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author gaozhaolu
 * @date 2019/7/12 19:31
 */
public class InputStreamReaderRunnable implements Runnable {
    private BufferedReader reader;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
    }

    @Override
    public void run() {
        System.out.println("进入 " + name + ":...");
        try {
            String line = reader.readLine();
            while (line != null) {
                // System.out.println("打印" + name + "..");
                System.out.println(line);
                line = reader.readLine();
            }
            reader.close();
            System.out.println("打印" + name + "完毕");
        } catch (IOException e) {
            System.out.println(name+"异常:" + e.getMessage());
        }
    }

}
