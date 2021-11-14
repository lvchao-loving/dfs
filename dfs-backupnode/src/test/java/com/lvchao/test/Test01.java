package com.lvchao.test;

import com.lvchao.dfs.backupnode.server.ThreadUntils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * @Title: Test01
 * @Package: com.lvchao.test
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/14 15:52
 * @version: V1.0
 */
public class Test01 {
    public static void main(String[] args) {
        Test01 test01 = new Test01();
        test01.readFile();
    }

    public void readFile(){
        String filePath = "F:\\backupnode\\test.meta";

        try (
            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
            FileInputStream fileInputStream = new FileInputStream(randomAccessFile.getFD());
            FileChannel channel = fileInputStream.getChannel();
        ){
            ByteBuffer buffer = ByteBuffer.allocate(2);
            StringBuilder fileContentSB = new StringBuilder();
            while (channel.read(buffer) > 0){
                buffer.flip();
                fileContentSB.append(StandardCharsets.UTF_8.decode(buffer).toString());
                buffer.clear();
            }
            ThreadUntils.println("读取文件内容成功：" + fileContentSB.toString());
        } catch (IOException e) {
            ThreadUntils.println("读取文件内容异常");
            e.printStackTrace();
        }
    }

    public void writeFile(){
        String content = "123456789qwertyuiop";
        String filePath = "F:\\backupnode\\test.meta";

        try (
            FileOutputStream out = new FileOutputStream(filePath,false);
            FileChannel channel = out.getChannel();
        ){

            ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());

            channel.write(buffer);
            channel.force(true);
            ThreadUntils.println("测试写入文件成功");
        } catch (IOException e) {
            ThreadUntils.println("测试下入文件异常");
            e.printStackTrace();
        }
    }
}
