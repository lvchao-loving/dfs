package com.lvchao.dfs.datanode.server;


import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Title: DataNodeNIOServer
 * @Package: com.lvchao.dfs.datanode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/16 21:12
 * @version: V1.0
 */
public class DataNodeNIOServer extends Thread{
    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    private Selector selector;

    private List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();

    private Map<String,CachedImage> cachedImageMap = new HashMap<>();

    /**
     * 与namenode节点进行同行的组件
     */
    private NameNodeRpcClient nameNodeRpcClient;
    /**
     * DataNodeNIOserver 节点初始化队列和线程的数量
     */
    private static final Integer  QUEUE_THREAD_NUMBER = 3;

    public DataNodeNIOServer(NameNodeRpcClient nameNodeRpcClient){
        ServerSocketChannel serverSocketChannel = null;
        try{
            this.nameNodeRpcClient = nameNodeRpcClient;
            // 初始化 ServerSocketChannel
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(dataNodeConfig.NIO_PORT),100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            // 初始化队列
            for (int i = 0; i < QUEUE_THREAD_NUMBER; i++) {
                queues.add(new LinkedBlockingQueue<SelectionKey>());
            }

            // 创建处理队列的线程
            for (int i = 0; i < QUEUE_THREAD_NUMBER; i++) {
                Worker worker = new Worker(queues.get(i));
                worker.setName("Work" + i);
                worker.start();
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
                    Filename filename = getFilename(socketChannel, buffer);
                    ThreadUtils.println("从网络请求中解析出来文件名：" + filename);
                    if (filename == null){
                        socketChannel.close();
                        continue;
                    }

                    // 从请求中解析文件大小
                    long imageLength = getImageLength(socketChannel,buffer);
                    ThreadUtils.println("从网络请求中解析出来文件长度：" + imageLength);
                    // 定义已经读取的文件大小
                    long hasReadImageLength = getHasReadImageLength(socketChannel);
                    ThreadUtils.println("初始化已经读取的文件大小：" + hasReadImageLength);

                    // 构建针对本地文件的输出流
                    FileOutputStream fileOutputStream = new FileOutputStream(filename.getAbsoluteFilename());
                    FileChannel fileChannel = fileOutputStream.getChannel();
                    // 根据现有文件的大小设置filechannel的position位置
                    fileChannel.position(fileChannel.size());

                    if (!cachedImageMap.containsKey(remoteAddr)){
                        hasReadImageLength += fileChannel.write(buffer);
                        ThreadUtils.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
                        buffer.clear();
                    }
                    int len = -1;
                    // 循环不断的从socketchannel中读取数据，并写入磁盘
                    while((len = socketChannel.read(buffer)) > 0){
                        // 将读取的数据加入到已经读取到的长度中
                        hasReadImageLength += len;
                        ThreadUtils.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
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
                        ThreadUtils.println("文件读取完毕，返回响应给客户端：" + remoteAddr);

                        nameNodeRpcClient.informReplicaReceived(filename.relativeFilename);

                        ThreadUtils.println("增量上传文件副本给NameNode节点...");
                    }else {
                        // 将未完成完整读取的图片数据添加到缓存中
                        CachedImage cachedImage = new CachedImage(filename, imageLength, hasReadImageLength);
                        cachedImageMap.put(remoteAddr,cachedImage);
                        key.interestOps(SelectionKey.OP_READ);
                        ThreadUtils.println("文件没有读取完毕，等待下一次OP_READ请求，缓存文件：" + cachedImage);
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
        private Filename getFilename(SocketChannel channel, ByteBuffer buffer) throws Exception{

            Filename filename = new Filename();

            String remoteAdd = channel.getRemoteAddress().toString();

            if (cachedImageMap.containsKey(remoteAdd)){
                filename = cachedImageMap.get(remoteAdd).getFilename();
            }else {
                String relativeFilename = getRelativeFilename(channel,buffer);
                if (relativeFilename == null){
                    return null;
                }
                // 设置相对路径
                filename.setRelativeFilename(relativeFilename);

                String[] relativeFilenameSplited = relativeFilename.split("/");
                String dirPath = dataNodeConfig.DATA_DIR;
                for (int i = 0; i < relativeFilenameSplited.length - 1; i++) {
                    if (StringUtils.isBlank(relativeFilenameSplited[i])){
                        continue;
                    }
                    dirPath += "\\" + relativeFilenameSplited[i];
                }
                // 判断文件路径是否存在不存在则创建
                File dir = new File(dirPath);
                if (!dir.exists()){
                    dir.mkdirs();
                }
                String absoluteFilename = dirPath + "\\" + relativeFilenameSplited[relativeFilenameSplited.length -1];
                filename.setAbsoluteFilename(absoluteFilename);
            }
            return filename;
        }
    }

    private long getHasReadImageLength(SocketChannel channel) throws Exception{
        long hasReadImageLength = 0L;
        String remoteAddr = channel.getRemoteAddress().toString();
        if (cachedImageMap.containsKey(remoteAddr)){
            hasReadImageLength = cachedImageMap.get(remoteAddr).getHasReadImageLength();
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

        return imageLength;
    }

    /**
     * 从网络请求中获取文件名
     * @param channel
     * @param buffer
     * @return
     */
    private String getRelativeFilename(SocketChannel channel, ByteBuffer buffer) throws Exception{
        String filename = null;
        int len = channel.read(buffer);
        if (len > 0){
            buffer.flip();

            byte[] filenameLengthBytes = new byte[4];
            buffer.get(filenameLengthBytes,0,4);

            ByteBuffer filenameLengthBuffer = ByteBuffer.allocate(4);
            filenameLengthBuffer.put(filenameLengthBytes);
            filenameLengthBuffer.flip();
            int filenameLength = filenameLengthBuffer.getInt();

            byte[] filenameBytes = new byte[filenameLength];
            buffer.get(filenameBytes,0,filenameLength);
            filename = new String(filenameBytes);
        }
        return filename;
    }

    /**
     * 封装文件名称对象
     */
    class Filename{
        /**
         * 相对路径
         */
        private String relativeFilename;
        /**
         * 绝对路径
         */
        private String absoluteFilename;

        public Filename() {
        }

        public Filename(String relativeFilename, String absoluteFilename) {
            this.relativeFilename = relativeFilename;
            this.absoluteFilename = absoluteFilename;
        }

        public String getRelativeFilename() {
            return relativeFilename;
        }

        public String getAbsoluteFilename() {
            return absoluteFilename;
        }

        public void setRelativeFilename(String relativeFilename) {
            this.relativeFilename = relativeFilename;
        }

        public void setAbsoluteFilename(String absoluteFilename) {
            this.absoluteFilename = absoluteFilename;
        }

        @Override
        public String toString() {
            return "Filename{" +
                    "relativeFilename='" + relativeFilename + '\'' +
                    ", absoluteFilename='" + absoluteFilename + '\'' +
                    '}';
        }
    }

    /**
     * 缓存正在发送的文件
     */
    class CachedImage{
        private Filename filename;
        private long imageLength;
        private long hasReadImageLength;

        public CachedImage() {
        }

        public CachedImage(Filename filename, long imageLength, long hasReadImageLength) {
            this.filename = filename;
            this.imageLength = imageLength;
            this.hasReadImageLength = hasReadImageLength;
        }

        public Filename getFilename() {
            return filename;
        }

        public long getImageLength() {
            return imageLength;
        }

        public long getHasReadImageLength() {
            return hasReadImageLength;
        }

        public void setFilename(Filename filename) {
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
                    "filename=" + filename +
                    ", imageLength=" + imageLength +
                    ", hasReadImageLength=" + hasReadImageLength +
                    '}';
        }
    }
}
