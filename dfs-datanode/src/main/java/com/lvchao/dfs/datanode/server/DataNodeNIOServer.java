package com.lvchao.dfs.datanode.server;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import static com.lvchao.dfs.datanode.server.DataNodeConfig.*;

/**
 * @Title: DataNodeNIOServer
 * @Package: com.lvchao.dfs.datanode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/16 21:12
 * @version: V1.0
 */
public class DataNodeNIOServer extends Thread{

    private Selector selector;

    private List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();

    private Map<String,CachedImage> cachedImageMap = new HashMap<>();
    /**
     * DataNodeNIOserver 节点初始化队列和线程的数量
     */
    private static final Integer  QUEUE_THREAD_NUMBER = 3;

    public DataNodeNIOServer(){
        ServerSocketChannel serverSocketChannel = null;
        try{
            // 初始化 ServerSocketChannel
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(NIO_PORT),100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            // 初始化队列
            for (int i = 0; i < QUEUE_THREAD_NUMBER; i++) {
                queues.add(new LinkedBlockingQueue<SelectionKey>());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while(iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    handleRequest(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 处理事件请求
     * @param key
     */
    private void handleRequest(SelectionKey key) {
        SocketChannel socketChannel = null;
        try{
            if (key.isAcceptable()){
                ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                socketChannel = serverSocketChannel.accept();
                if(socketChannel != null){
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector,SelectionKey.OP_READ);
                }
            }else if (key.isReadable()){
                socketChannel = (SocketChannel) key.channel();
                String remoteAddr = socketChannel.getRemoteAddress().toString();
                int queueIndex = remoteAddr.hashCode() % queues.size();
                queues.get(queueIndex).put(key);
            }
        }catch (Throwable throwable){
            throwable.printStackTrace();
            if (socketChannel != null){
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 真正处理网络请求的工作线程，一个线程处理一个队列
     */
    class Worker extends Thread{
        private LinkedBlockingQueue<SelectionKey> queue;

        public Worker(LinkedBlockingQueue<SelectionKey> queue){
            this.queue = queue;
        }

        @Override
        public void run() {
            while (true){
                SocketChannel channel = null;
                try {
                    // 阻塞队列中存在SelectionKey则向下运行，否则阻塞
                    SelectionKey key = queue.take();
                    channel = (SocketChannel) key.channel();
                    if (!channel.isOpen()){
                        channel.close();
                        continue;
                    }
                    String remoteAddr = channel.getRemoteAddress().toString();
                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
                    int len = -1;
                    String filename = null;
                    long imageLength = 0;
                    if (cachedImageMap.containsKey(remoteAddr)){
                        CachedImage cachedImage = cachedImageMap.get(remoteAddr);
                        filename = cachedImage.getFilename();
                        imageLength = cachedImage.imageLength;
                    }else {
                        // 生成临时文件名称
                        filename = "F:\\dfs\\" + UUID.randomUUID().toString() + ".jpg";

                        // 计算传入文件的长度
                        len = channel.read(buffer);
                        // 确保传递过来并且接收成功图片字节长度，赋值到 imageLength 字段
                        if (len > 8){
                            byte[] imageLengthBytes = new byte[8];
                            buffer.flip();
                            buffer.get(imageLengthBytes,0,8);
                            ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
                            imageLengthBuffer.put(imageLengthBytes);
                            imageLengthBuffer.flip();
                            imageLength = imageLengthBuffer.getLong();
                        } else if (len <= 0){
                            // 说明传入的文件不正确，关闭socket
                            channel.close();
                            continue;
                        }
                    }
                    // 已经读取的字段
                    long hasReadImageLength = 0;
                    if (cachedImageMap.containsKey(remoteAddr)){
                        // 如果上次读取过则更新上次读取的位置
                        hasReadImageLength = cachedImageMap.get(remoteAddr).getHasReadImageLength();
                    }
                    FileOutputStream outputStream = new FileOutputStream(filename);
                    FileChannel fileChannel = outputStream.getChannel();
                    fileChannel.position(fileChannel.size());

                    // 如果文件没有存储过
                    if (!cachedImageMap.containsKey(remoteAddr)){
                        buffer.flip();
                        hasReadImageLength += fileChannel.write(buffer);
                        buffer.clear();
                    }

                    while ((len = channel.read(buffer)) > 0){
                        hasReadImageLength += len;
                        buffer.flip();
                        fileChannel.write(buffer);
                        buffer.clear();
                    }

                    if (cachedImageMap.get(remoteAddr) != null){
                        if (hasReadImageLength == cachedImageMap.get(remoteAddr).getHasReadImageLength()){
                            channel.close();
                            continue;
                        }
                    }else {
                        CachedImage cachedImage = new CachedImage(filename, imageLength, hasReadImageLength);
                        cachedImageMap.put(remoteAddr,cachedImage);
                        key.interestOps(SelectionKey.OP_READ);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    if (channel != null){
                        try {
                            channel.close();
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    class CachedImage{
        private String filename;
        private long imageLength;
        private long hasReadImageLength;

        public CachedImage() {
        }

        public CachedImage(String filename, long imageLength, long hasReadImageLength) {
            this.filename = filename;
            this.imageLength = imageLength;
            this.hasReadImageLength = hasReadImageLength;
        }

        public String getFilename() {
            return filename;
        }

        public long getImageLength() {
            return imageLength;
        }

        public long getHasReadImageLength() {
            return hasReadImageLength;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public void setImageLength(long imageLength) {
            this.imageLength = imageLength;
        }

        public void setHasReadImageLength(long hasReadImageLength) {
            this.hasReadImageLength = hasReadImageLength;
        }

        @Override
        public String toString() {
            return "CachedImage{" +
                    "filename='" + filename + '\'' +
                    ", imageLength=" + imageLength +
                    ", hasReadImageLength=" + hasReadImageLength +
                    '}';
        }
    }
}
