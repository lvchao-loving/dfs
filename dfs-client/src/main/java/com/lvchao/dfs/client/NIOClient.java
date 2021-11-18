package com.lvchao.dfs.client;

import org.apache.commons.lang3.ThreadUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * @Title: NIOClient
 * @Package: com.lvchao.dfs.client
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/16 17:20
 * @version: V1.0
 */
public class NIOClient {

    public static void sendFile(byte[] file, long fileSize){
        try(
            SocketChannel socketChannel = SocketChannel.open();
            Selector selector = Selector.open();
                ){
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress("localhost",9000));

            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while (sending){
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (key.isConnectable()){
                        SocketChannel channel = (SocketChannel) key.channel();
                        if (channel.isConnectionPending()){
                            channel.finishConnect();
                            ByteBuffer wrap = ByteBuffer.wrap("hello ,my firend!".getBytes());
                            channel.write(wrap);
                            channel.register(selector,SelectionKey.OP_READ);
                        }
                    }else if (key.isReadable()){
                        SocketChannel channel = (SocketChannel) key.channel();
                        // 读取响应数据
                        StringBuilder fileContentSB = new StringBuilder();
                        ByteBuffer buffer = ByteBuffer.allocate(2);
                        while (channel.read(buffer) > 0){
                            buffer.flip();
                            fileContentSB.append(StandardCharsets.UTF_8.decode(buffer).toString());
                            buffer.clear();
                        }
                        String channelContent = fileContentSB.toString();
                        ThreadUntils.println("接收到的服务消息为：" + channelContent);
                        sending = false;
                    }
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /*public static void sendFile1(byte[] file, long fileSize) {
        try (
                SocketChannel channel = SocketChannel.open();
                Selector selector = Selector.open();
                ){

            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress("localhost", 9000));

            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while(sending){
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while(keysIterator.hasNext()){
                    SelectionKey key = (SelectionKey) keysIterator.next();
                    keysIterator.remove();

                    if(key.isConnectable()){
                        channel = (SocketChannel) key.channel();

                        if(channel.isConnectionPending()){
                            channel.finishConnect();

                            long imageLength = fileSize;

                            ByteBuffer buffer = ByteBuffer.allocate((int)imageLength * 2);
                            buffer.putLong(imageLength); // long对应了8个字节，放到buffer里去
                            buffer.put(file);

                            channel.register(selector, SelectionKey.OP_READ);
                        }
                    } else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();

                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int len = channel.read(buffer);

                        if(len > 0) {
                            System.out.println("[" + Thread.currentThread().getName()
                                    + "]收到响应：" + new String(buffer.array(), 0, len));
                            sending = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
