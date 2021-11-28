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
     * 发送文件标识
     */
    public static final Integer SEND_FILE = 1;
    /**
     * 读取文件标识
     */
    public static final Integer READ_FILE = 2;

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
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + filenameBytes.length + 8 + fileSize.intValue() + 8);
        byteBuffer.putInt(SEND_FILE);
        byteBuffer.putInt(filenameBytes.length);
        byteBuffer.put(filename.getBytes());
        byteBuffer.putLong(fileSize);
        byteBuffer.put(file);
        byteBuffer.flip();
        return byteBuffer;
    }

    /**
     * 短链接读取文件
     * @param hostname
     * @param nioPort
     * @param filename
     */
    public void readFile(String hostname, Integer nioPort, String filename){
        SocketChannel socketChannel = null;
        Selector selector = null;

        try {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(hostname,nioPort));

            selector = Selector.open();
            socketChannel.register(selector,SelectionKey.OP_CONNECT);

            boolean reading = true;

            while (reading){
                selector.select();

                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (key.isConnectable()){
                        socketChannel = (SocketChannel) key.channel();

                        if (socketChannel.isConnectionPending()){
                            // 如果正在连接则进入完成三次握手，TCP连接建立完成
                            socketChannel.finishConnect();
                        }

                        int filenameLength = filename.getBytes().length;
                        // 4-位读数据标识长度  4-文件名称长度  n-真正文件长度
                        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + filenameLength);
                        byteBuffer.putInt(READ_FILE);
                        byteBuffer.putInt(filenameLength);
                        byteBuffer.put(filename.getBytes());
                        byteBuffer.flip();
                        int sentData = socketChannel.write(byteBuffer);
                        ThreadUtils.println("已经发送了" + sentData + "字节的数据到" + hostname + "机器的" + nioPort + "端口上");
                        // 注册读请求事件
                        socketChannel.register(selector,SelectionKey.OP_READ);
                    }else if (key.isReadable()){
                        socketChannel = (SocketChannel) key.channel();
                        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                        int len = socketChannel.read(byteBuffer);
                        if (len > 0){
                            byte[] array = byteBuffer.array();
                            ThreadUtils.println("接收到响应数据：" + new String(array,0,len));
                            reading = false;
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (socketChannel != null){
                try{
                    socketChannel.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            if (selector != null){
                try {
                    selector.close();
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
