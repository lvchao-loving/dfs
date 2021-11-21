package com.lvchao.dfs.client;

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
   /**
     * 短链接发送数据
     * @param file
     * @param fileSize
     */
    public static void sendFile(String hostname, Integer nioPort, byte[] file, String filename, Long fileSize){
        try(
            SocketChannel socketChannel = SocketChannel.open();
            Selector selector = Selector.open();
                ){
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(hostname,nioPort));

            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while (sending){
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    // NIO 是否允许进行连接
                    if (key.isConnectable()){
                        SocketChannel channel = (SocketChannel) key.channel();
                        // NIO 是否正在连接
                        if (channel.isConnectionPending()){
                            // 等待三次握手的完成
                            channel.finishConnect();
                            ByteBuffer buffer = loadBufferData(file,filename,fileSize);
                            int write = channel.write(buffer);
                            ThreadUtils.println("已经发送了" + write + "字节的数据");
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
                        ThreadUtils.println("接收到的服务消息为：" + channelContent);
                        sending = false;
                    }
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 封装发送数据
     * @param file
     * @param filename
     * @param fileSize
     * @return
     */
    private static ByteBuffer loadBufferData(byte[] file, String filename, Long fileSize) {
        // 计算分配 ByteBuffer 长度
        byte[] filenameBytes = filename.getBytes();
        /**
         * 计算分配bytebuffer的长度 = 4 + filenameBytes.length + 8 + fileSize.intValue() + 8
         * 计算分配bytebuffer的长度 = 文件名称长度 + 文件名称 + 文件长度 + 文件 + 扩展8字节
         */
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + filenameBytes.length + 8 + fileSize.intValue() + 8);
        byteBuffer.putInt(filenameBytes.length);
        byteBuffer.put(filename.getBytes());
        byteBuffer.putLong(fileSize);
        byteBuffer.put(file);
        byteBuffer.flip();
        return byteBuffer;
    }
}
