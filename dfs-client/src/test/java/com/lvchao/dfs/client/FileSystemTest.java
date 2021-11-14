package com.lvchao.dfs.client;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Title: FileSystemTest
 * @Package: com.lvchao.dfs.client
 * @Description:
 * @auther: chao.lv
 * @date: 2021/10/22 22:10
 * @version: V1.0
 */
public class FileSystemTest {
    public static AtomicInteger atomicInteger = new AtomicInteger(1);

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = new FileSystemImpl();
       /* for (int i = 1; i <= 10; i++) {
            new Thread(()->{
                Random random = new Random();
                for (int j = 1; j <= 100; j++) {
                    try {
                        fileSystem.mkdir("/usr/local/lvchao" + atomicInteger.getAndIncrement());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },"threadName" + i).start();
        }*/

        fileSystem.shutdown();
    }
}
