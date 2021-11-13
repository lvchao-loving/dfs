package com.lvchao.dfs.client;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Title: FileSystemTest
 * @Package: com.lvchao.dfs.client
 * @Description:
 * @auther: chao.lv
 * @date: 2021/10/22 22:10
 * @version: V1.0
 */
public class FileSystemTest {
    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = new FileSystemImpl();
        for (int i = 1; i <= 10; i++) {
            new Thread(()->{
                Random random = new Random();
                for (int j = 1; j <= 100; j++) {
                    try {
                        fileSystem.mkdir("/usr/local/lvchao" + j + Thread.currentThread().getName());
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },"threadName" + i).start();
        }

       // fileSystem.shutdown();
    }
}
