package com.lvchao.dfs.namenode.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * @Title: FSImageUploadServer
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 负责 fsimage 文件上传的server
 * @auther: chao.lv
 * @date: 2021/10/31 19:48
 * @version: V1.0
 */
public class FSImageUploadServer extends Thread{

    private Selector selector;

    public FSImageUploadServer(){
        this.init();
    }

    private void init(){
        ServerSocketChannel serverSocketChannel = null;
        try{

            // 创建 Selector
            selector = Selector.open();
            // 创建 ServerSocketChannel
            serverSocketChannel = ServerSocketChannel.open();
            // 设置为非阻塞模式
            serverSocketChannel.configureBlocking(false);
            // 绑定服务端口，backlog 设置了队列中最大连接数量
            serverSocketChannel.socket().bind(new InetSocketAddress(9000), 100);
            // 注册 selector的SelectionKey.OP_ACCEPT事件
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        ThreadUntils.println("FSImageUploadServer 成功启动...");
        while (true){
            try{
                // 当 selector 中没有对应的 selectionKey 事件触发时阻塞
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while (keysIterator.hasNext()){
                    SelectionKey key = keysIterator.next();
                    // 将处理后的 SelectionKey 事件移除
                    keysIterator.remove();
                    try {
                        handleRequest(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }catch (Exception exception){
                exception.printStackTrace();
            }
        }
    }

    /**
     * 处理网络请求总方法
     * @param key
     */
    private void handleRequest(SelectionKey key) throws IOException {
        if (key.isAcceptable()){
            handleAcceptableRequest(key);
        } else if (key.isReadable()){
            handleReadableRequest(key);
        } else if (key.isWritable()){
            handleWritableRequest(key);
        }
    }

    /**
     * 处理返回响应给BackupNode
     * @param key
     */
    private void handleWritableRequest(SelectionKey key) throws IOException {
        SocketChannel socketChannel = null;

        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            buffer.put("SUCCESS".getBytes());
            buffer.flip();

            socketChannel = (SocketChannel) key.channel();
            socketChannel.write(buffer);

            socketChannel.register(selector, SelectionKey.OP_READ);
        } catch (Exception e) {
            e.printStackTrace();
            if(socketChannel != null) {
                socketChannel.close();
            }
        }
    }

    /**
     * 处理发送fsimage文件的请求
     * @param key
     */
    private void handleReadableRequest(SelectionKey key) throws IOException {
        SocketChannel socketChannel = null;
        try {
            socketChannel = (SocketChannel) key.channel();
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            int read = socketChannel.read(buffer);
            // 当 read为-1 时说明正常断开连接
            if (read == -1){
                ThreadUntils.println("正常关闭key");
                key.cancel();
                return;
            }
            // 先把上一次的fsimage文件删除
            String fsimageFilePath = "F:\\editslog\\fsimage.meta";
            File fsimageFile = new File(fsimageFilePath);
            if(fsimageFile.exists()) {
                fsimageFile.delete();
            }
            try (
                    RandomAccessFile fsimageImageRAF = new RandomAccessFile(fsimageFilePath, "rw");
                    FileOutputStream fsimageOut = new FileOutputStream(fsimageImageRAF.getFD());
                    FileChannel fsimageFileChannel = fsimageOut.getChannel();
            ){
                if (read == 0){
                    ThreadUntils.println("没有读取到任何数据！");
                    return;
                }
                do {
                    buffer.flip();
                    buffer.rewind();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();
                }while (socketChannel.read(buffer) > 0);
                fsimageFileChannel.force(true);
                socketChannel.register(selector, SelectionKey.OP_WRITE);
            } catch (Exception e){
                // 异常情况下取消事件处理（因为客户端断开了，因此需要将key取消，从selector的keys集合中真正删除 key）
                key.cancel();
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(socketChannel != null) {
                socketChannel.close();
            }
        }
    }

    /**
     * 处理 BackupNode 连接请求
     * @param key
     */
    private void handleAcceptableRequest(SelectionKey key) throws IOException {
        SocketChannel socketChannel = null;
        try {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            socketChannel = serverSocketChannel.accept();
            if (socketChannel != null){
                socketChannel.configureBlocking(false);
                socketChannel.register(selector,SelectionKey.OP_READ);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (socketChannel != null){
                socketChannel.close();
            }
        }
    }
}
