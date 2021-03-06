package com.lvchao.dfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
       /* for (int i = 0; i < 10; i++) {
            testCreateFile();
        }*/
      //  testCreateFile();
        testReadFile();

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

        fileChannel.close();
        fileInputStream.close();

        String filename = UUID.randomUUID().toString();
        ThreadUtils.println("????????????????????????" + fileLength + "???????????????????????????" + filename);
        // filesystem.upload(buffer.array(), "/image/product/" + filename + ".jpg",fileLength);
        filesystem.upload(buffer.array(), "/image/product/iphone.jpg",fileLength);
    }

    private static void testReadFile() throws Exception {
        byte[] fileByte = filesystem.download("/image/product/iphone.jpg");
        ByteBuffer buffer = ByteBuffer.wrap(fileByte);
        FileOutputStream imageOut = new FileOutputStream("F:\\tmp\\iphone.jpg");
        FileChannel imageChannel = imageOut.getChannel();
        imageChannel.write(buffer);

        imageChannel.close();
        imageOut.close();
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
            System.out.println("??????????????????????????????" + len);
            buffer.flip();
            stringBuilder.append(StandardCharsets.UTF_8.decode(buffer).toString());
            buffer.clear();
        }
        System.out.println(stringBuilder.toString());

        // filesystem.upload(null, "/image/product/iphone001.jpg",512L);
    }*/
}
