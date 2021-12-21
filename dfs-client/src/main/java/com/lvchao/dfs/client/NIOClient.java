package com.lvchao.dfs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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
     * 短链接-发送数据
     * @param hostname
     * @param nioPort
     * @param file
     * @param filename
     * @param fileSize
     */
    public Boolean sendFile(String hostname, Integer nioPort, byte[] file, String filename, Long fileSize){
        SocketChannel socketChannel = null;
        Selector selector = null;
        try {
            selector = Selector.open();

            socketChannel = SocketChannel.open();
            // 使用selector时，必须设置成非阻塞
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(hostname,nioPort));

            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;
            ByteBuffer buffer = null;

            while (sending){
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    // NIO 是否允许进行连接
                    if (key.isConnectable()){
                        socketChannel = (SocketChannel) key.channel();
                        // NIO 是否正在连接
                        if (socketChannel.isConnectionPending()){
                            // 等待三次握手的完成，连接建立好
                            while (!socketChannel.finishConnect()){
                                TimeUnit.MILLISECONDS.sleep(100);
                            }
                        }
                        buffer = loadBufferData(file,filename,fileSize);
                        int writeDataLen = socketChannel.write(buffer);
                        ThreadUtils.println("已经发送了" + writeDataLen + "bytes的数据到服务端");
                        if (buffer.hasRemaining()){
                            ThreadUtils.println("本次数据包没有发送完毕，下次回继续发送...");
                            key.interestOps(SelectionKey.OP_WRITE);
                        }else {
                            ThreadUtils.println("本次数据包发送完毕，准备读取服务端的响应");
                            key.interestOps(SelectionKey.OP_READ);
                        }
                    } else if (key.isWritable()){
                        socketChannel = (SocketChannel) key.channel();
                        int writeDataLen = socketChannel.write(buffer);
                        ThreadUtils.println("再一次发送了" + writeDataLen + "bytes的数据到服务端");
                        if (!buffer.hasRemaining()){
                            ThreadUtils.println("本次数据包没有发送完毕，下次回继续发送...");
                            key.interestOps(SelectionKey.OP_READ);
                        }else {
                            ThreadUtils.println("发送完了数据到服务端");
                        }
                    } else if (key.isReadable()){
                        socketChannel = (SocketChannel) key.channel();
                        // 读取响应数据
                        StringBuilder fileContentSB = new StringBuilder();
                        ByteBuffer readBuffer = ByteBuffer.allocate(8);
                        while (socketChannel.read(readBuffer) > 0){
                            readBuffer.flip();
                            fileContentSB.append(new String(readBuffer.array()));
                            readBuffer.clear();
                        }
                        String channelContent = fileContentSB.toString();
                        ThreadUtils.println("接收到的服务消息为：" + channelContent);
                        sending = false;
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            return false;
        } finally {
            if (socketChannel != null){
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (selector != null){
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    /**
     * 封装发送数据
     * @param file
     * @param filename
     * @param fileSize
     * @return
     */
    public ByteBuffer loadBufferData(byte[] file, String filename, Long fileSize) {
        // 计算分配 ByteBuffer 长度
        byte[] filenameBytes = filename.getBytes();
        /**
         * 计算分配bytebuffer的长度 = 4 + 4 + filenameBytes.length + 8 + fileSize.intValue()
         * 计算分配bytebuffer的长度 = 发送消息类型 + 文件名称长度 + 文件名称 + 文件长度 + 文件
         */
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + filenameBytes.length + 8 + fileSize.intValue());
        byteBuffer.putInt(SEND_FILE);
        byteBuffer.putInt(filenameBytes.length);
        byteBuffer.put(filename.getBytes());
        byteBuffer.putLong(fileSize);
        byteBuffer.put(file);
        byteBuffer.flip();
        return byteBuffer;
    }

    /**
     * 短链接-读取文件
     * @param hostname
     * @param nioPort
     * @param filename
     */
    public byte[] readFile(String hostname, Integer nioPort, String filename) throws Exception{
        SocketChannel socketChannel = null;
        Selector selector = null;
        byte[] byteArray = null;
        try {
            selector = Selector.open();

            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(hostname,nioPort));

            socketChannel.register(selector,SelectionKey.OP_CONNECT);

            boolean reading = true;
            Long fileLength = null;
            ByteBuffer fileLengthBuffer = null;
            ByteBuffer fileBuffer = null;
            while (reading){
                selector.select();

                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (key.isConnectable()){
                        socketChannel = (SocketChannel) key.channel();

                        // NIO 是否正在连接
                        if (socketChannel.isConnectionPending()){
                            // 等待三次握手的完成，连接建立好
                            while (!socketChannel.finishConnect()){
                                TimeUnit.MILLISECONDS.sleep(100);
                            }
                        }

                        int filenameLength = filename.getBytes().length;
                        // 4-位读数据标识长度  4-文件名称长度  n-真正文件长度
                        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + filenameLength);
                        byteBuffer.putInt(READ_FILE);
                        byteBuffer.putInt(filenameLength);
                        byteBuffer.put(filename.getBytes());
                        byteBuffer.flip();
                        int sentDataLen = socketChannel.write(byteBuffer);
                        ThreadUtils.println("已经发送了" + sentDataLen + "字节的数据到" + hostname + "机器的" + nioPort + "端口上");
                        // 注册读请求事件
                        socketChannel.register(selector,SelectionKey.OP_READ);
                    }else if (key.isReadable()){
                        socketChannel = (SocketChannel) key.channel();

                        if (Objects.isNull(fileLength)){
                            if (Objects.isNull(fileLengthBuffer)){
                                fileLengthBuffer = ByteBuffer.allocate(8);
                            }
                            socketChannel.read(fileLengthBuffer);
                            if (!fileLengthBuffer.hasRemaining()) {
                                fileLengthBuffer.flip();
                                fileLength = fileLengthBuffer.getLong();
                            } else {
                                continue;
                            }
                        }
                        if (Objects.isNull(fileBuffer)){
                            fileBuffer = ByteBuffer.allocate(fileLength.intValue());
                        }
                        socketChannel.read(fileBuffer);
                        if (!fileBuffer.hasRemaining()) {
                            fileBuffer.flip();
                            byteArray = fileBuffer.array();
                            reading = false;
                        } else {
                            continue;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("读取图片异常...");
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
        return byteArray;
    }
}
