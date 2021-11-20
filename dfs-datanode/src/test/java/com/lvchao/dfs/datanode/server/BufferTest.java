package com.lvchao.dfs.datanode.server;

import java.nio.ByteBuffer;

/**
 * @Title: BufferTest
 * @Package: com.lvchao.dfs.datanode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/19 20:14
 * @version: V1.0
 */
public class BufferTest {
    public static void main(String[] args) {
        String message = "123453367890";
      //  ByteBuffer wrap = ByteBuffer.wrap(message.getBytes());
        ByteBuffer wrap = ByteBuffer.allocate(8);
        wrap.putInt(333333333);
        //ByteBuffer allocate = ByteBuffer.allocate(4);
        byte[] bytes = new byte[4];
        wrap.flip();
        wrap.get(bytes,0,4);

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(bytes[i]);
        }
        System.out.println("赋值后：----------------------");
        ByteBuffer allocate = ByteBuffer.allocate(4);
        allocate.put(bytes);
        allocate.flip();
        int anInt = allocate.getInt();
        System.out.println(anInt);
    }
}
