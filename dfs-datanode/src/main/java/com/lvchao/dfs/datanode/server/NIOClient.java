package com.lvchao.dfs.datanode.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
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
     * 读取文件标识
     */
    public static final Integer READ_FILE = 2;

    /**
     * 短链接-读取文件
     * @param hostname
     * @param nioPort
     * @param filename
     */
    public byte[] readFile(String hostname, Integer nioPort, String filename){
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
