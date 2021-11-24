package com.lvchao.dfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
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

    private static FileSystem filesystem = new FileSystemImpl();

    public static void main(String[] args) throws Exception {
        // testMkdir();
        // testShutdown();
        for (int i = 0; i < 10; i++) {
            testCreateFile();
        }

    }

    private static void testMkdir() throws Exception {
        for (int i = 1; i <= 10; i++) {
            new Thread(()->{
                Random random = new Random();
                for (int j = 1; j <= 100; j++) {
                    try {
                        filesystem.mkdir("/usr/local/lvchao1" + atomicInteger.getAndIncrement());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },"threadName" + i).start();
        }
    }

    private static void testShutdown() throws Exception {
        filesystem.shutdown();
    }

    /**
     *
     * @throws Exception
     */
    private static void testCreateFile() throws Exception {
        File file = new File("F:\\tmp\\lvchao.jpg");
        Long fileLength = file.length();
        FileInputStream fileInputStream = new FileInputStream(file);
        FileChannel fileChannel = fileInputStream.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(fileLength.intValue());
        fileChannel.read(buffer);
        buffer.flip();
        int length = buffer.array().length;

        fileChannel.close();
        fileInputStream.close();
        ThreadUtils.println("发送的文件长度：" + fileLength + "，发送文件的名称：iphone001.jpg");
        String s = UUID.randomUUID().toString();
        filesystem.upload(buffer.array(), "/image/product/iphone" + s + ".jpg",fileLength);
    }
   /* private static void testCreateFile() throws Exception {
       // File file = new File("F:\\tmp\\lvchao.jpg");
        File file = new File("F:\\tmp\\lvchao.txt");
        FileInputStream fileInputStream = new FileInputStream(file);
        FileChannel fileChannel = fileInputStream.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(2);
        int len = 0;
        StringBuilder stringBuilder = new StringBuilder();
        while((len = fileChannel.read(buffer)) > 0){
            System.out.println("当前读到的数据长度：" + len);
            buffer.flip();
            stringBuilder.append(StandardCharsets.UTF_8.decode(buffer).toString());
            buffer.clear();
        }
        System.out.println(stringBuilder.toString());

        // filesystem.upload(null, "/image/product/iphone001.jpg",512L);
    }*/
}
