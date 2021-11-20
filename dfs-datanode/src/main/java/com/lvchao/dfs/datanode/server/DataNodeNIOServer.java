package com.lvchao.dfs.datanode.server;

import java.io.File;
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
                SocketChannel socketChannel = null;
                try {
                    // 阻塞队列中存在SelectionKey则向下运行，否则阻塞
                    SelectionKey key = queue.take();
                    socketChannel = (SocketChannel) key.channel();
                    if (!socketChannel.isOpen()){
                        socketChannel.close();
                        continue;
                    }
                    String remoteAddr = socketChannel.getRemoteAddress().toString();

                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
                    // 从请求头中解析文件名称
                    String filename = getFilename(socketChannel, buffer);

                    if (filename == null){
                        continue;
                    }

                    // 从请求中解析文件大小
                    long imageLength = getImageLength(socketChannel,buffer);

                    // 定义已经读取的文件大小
                    long hasReadImageLength = getHasReadImageLength(socketChannel);

                    FileOutputStream fileOutputStream = new FileOutputStream(filename);
                    FileChannel fileChannel = fileOutputStream.getChannel();
                    // ?
                    fileChannel.position(fileChannel.size());

                    if (cachedImageMap.containsKey(remoteAddr)){
                        hasReadImageLength += fileChannel.write(buffer);
                        buffer.clear();
                    }
                    int len = -1;
                    // 循环不断的从socketchannel中读取数据，并写入磁盘
                    while((len = socketChannel.read(buffer)) > 0){
                        // 将读取的数据加入到已经读取到的长度中
                        hasReadImageLength += len;
                        // 将 buffer 中的数据写入到缓存中
                        buffer.flip();
                        fileChannel.write(buffer);
                        buffer.clear();
                    }
                    // 执行到这里说明已经读取完当前socketchannel中的数据（并不代表读取到了完整的图片数据），则关闭 Filechannel 和 FileOutputStream
                    fileChannel.close();
                    fileOutputStream.close();

                    // 说明读取到了完整的数据
                    if (hasReadImageLength == imageLength){
                        ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
                        socketChannel.write(outBuffer);
                        cachedImageMap.remove(remoteAddr);
                    }else {
                        // 将未完成完整读取的图片数据添加到缓存中
                        CachedImage cachedImage = new CachedImage(filename, imageLength, hasReadImageLength);
                        cachedImageMap.put(remoteAddr,cachedImage);
                        key.interestOps(SelectionKey.OP_READ);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (socketChannel != null){
                        try {
                            socketChannel.close();
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                    }
                }
            }
        }

        /**
         * 返回文件名称
         * @param channel
         * @param buffer
         * @return
         */
        private String getFilename(SocketChannel channel, ByteBuffer buffer) throws Exception{
            String filename = null;
            String remoteAddress = channel.getRemoteAddress().toString();
            if (cachedImageMap.containsKey(remoteAddress)){
                filename = cachedImageMap.get(remoteAddress).getFilename();
            }else {
                filename = getFilenameFromChannel(channel,buffer);
                if (filename == null){
                    return null;
                }
                String[] filenameSplited = filename.split("/");
                String dirPath = DATA_DIR;
                for (int i = 0; i < filenameSplited.length - 1; i++) {
                    if ("/".equals(filenameSplited[i])){
                        continue;
                    }
                    dirPath += "\\" + filenameSplited[i];
                }
                // 判断文件路径是否存在不存在则创建
                File dir = new File(dirPath);
                if (!dir.exists()){
                    dir.mkdirs();
                }
                filename = dirPath + "\\" + filenameSplited[filenameSplited.length -1];
            }
            return filename;
        }
    }

    private long getHasReadImageLength(SocketChannel channel) throws Exception{
        long hasReadImageLength = 0L;
        String remoteAddr = channel.getRemoteAddress().toString();
        if (cachedImageMap.containsKey(remoteAddr)){
            hasReadImageLength = cachedImageMap.get(remoteAddr).getImageLength();
        }
        return hasReadImageLength;
    }

    private long getImageLength(SocketChannel channel, ByteBuffer buffer) throws Exception{
        Long imageLength = 0L;
        String remoteAddr = channel.getRemoteAddress().toString();
        if (cachedImageMap.containsKey(remoteAddr)){
            imageLength = cachedImageMap.get(remoteAddr).getImageLength();
        }else {
            byte[] imageLengthBytes = new byte[8];
            buffer.get(imageLengthBytes,0,8);

            ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
            imageLengthBuffer.put(imageLengthBytes);
            imageLengthBuffer.flip();
            imageLength = imageLengthBuffer.getLong();
        }

        return 0;
    }

    /**
     * 从网络请求中获取文件名
     * @param channel
     * @param buffer
     * @return
     */
    private String getFilenameFromChannel(SocketChannel channel, ByteBuffer buffer) throws Exception{
        String filename = null;
        int len = channel.read(buffer);
        if (len > 0){
            byte[] filenameLengthBytes = new byte[4];
            buffer.get(filenameLengthBytes,0,4);

            ByteBuffer filenameLengthBuffer = ByteBuffer.allocate(4);
            filenameLengthBuffer.put(filenameLengthBytes);
            filenameLengthBuffer.flip();
            int filenameLength = filenameLengthBuffer.getInt();

            byte[] filenameBytes = new byte[filenameLength];
            buffer.get(filenameBytes,4,filenameLength);
            filename = new String(filenameBytes);
        }
        return filename;
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
