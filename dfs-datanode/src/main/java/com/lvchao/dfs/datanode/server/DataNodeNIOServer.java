package com.lvchao.dfs.datanode.server;


import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    /**
     * 发送文件标识
     */
    public static final Integer SEND_FILE = 1;
    /**
     * 读取文件标识
     */
    public static final Integer READ_FILE = 2;

    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    private Selector selector;
    /**
     * 与namenode节点进行同行的组件
     */
    private NameNodeRpcClient nameNodeRpcClient;
    /**
     * DataNodeNIOserver 节点初始化队列和线程的数量
     */
    private static final Integer  QUEUE_THREAD_NUMBER = 3;

    private List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();
    /**
     * 缓存的没读取完的文件数据
     */
    private Map<String, CachedRequest> cachedRequestMap = new ConcurrentHashMap<>();
    /**
     * 处理 requestType 拆包问题
     */
    private Map<String, ByteBuffer> requestTyptByClient = new ConcurrentHashMap<>();
    /**
     * 处理 filenameLength 拆包问题
     */
    private Map<String, ByteBuffer> filenameLengthByClient = new ConcurrentHashMap<>();
    /**
     * 处理 filename 拆包问题
     */
    private Map<String, ByteBuffer> filenameByClient = new ConcurrentHashMap<>();
    /**
     * 处理 fileLength 拆包问题
     */
    private Map<String, ByteBuffer> fileLengthByClient = new ConcurrentHashMap<>();
    /**
     * 处理 file 拆包问题
     */
    private Map<String, ByteBuffer> fileByClient = new ConcurrentHashMap<>();

    /**
     * 初始化 网络组件
     * @param nameNodeRpcClient
     */
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
                    // 处理事件
                    handleEvents(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 处理事件分发
     * @param key
     * @throws Exception
     */
    private void handleEvents(SelectionKey key) throws IOException {
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
                socketChannel.close();
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
                    // 阻塞队列中存在SelectionKey则向下运行
                    SelectionKey key = queue.take();
                    socketChannel = (SocketChannel) key.channel();

                    handleRequest(socketChannel, key);
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
    }

    /**
     * 处理客户端发送过来的请求
     * @param socketChannel
     * @param key
     */
    private void handleRequest(SocketChannel socketChannel, SelectionKey key) throws Exception{

        String client = socketChannel.getRemoteAddress().toString();
        ThreadUtils.println("接收到客户端的请求：" + client);

        // 根据不同的请求类型进行处理
        if (cachedRequestMap.containsKey(client)){
            handleSendFileRequest(socketChannel, key);
        } else {
            // 获取消息类型
            Integer requestType = getRequestType(socketChannel);
            if (SEND_FILE.equals(requestType)){
                handleSendFileRequest(socketChannel, key);
            } else if (READ_FILE.equals(requestType)){
                handleReadFileRequest(socketChannel, key);
            }
        }
    }

    /**
     * 获取本次消息的类型
     * @param socketChannel
     * @return
     * @throws Exception
     */
    public Integer getRequestType(SocketChannel socketChannel) throws Exception {
        String client = socketChannel.getRemoteAddress().toString();
        // 判断缓存中是否存在，存在则直接返回
        Integer requestType = getCachedRequest(client).getRequestType();
        if (Objects.nonNull(requestType)){
            return requestType;
        }
        ByteBuffer byteBuffer = requestTyptByClient.get(client);
        if (Objects.isNull(byteBuffer)){
            byteBuffer = ByteBuffer.allocate(4);
        }
        socketChannel.read(byteBuffer);
        if (!byteBuffer.hasRemaining()){
            byteBuffer.flip();
            requestType = byteBuffer.getInt();
            getCachedRequest(client).setRequestType(requestType);
            requestTyptByClient.remove(client);
        }else {
            requestTyptByClient.put(client, byteBuffer);
        }
        return requestType;
    }

    /**
     * 发送文件请求
     * @param socketChannel
     * @param key
     */
    private void handleSendFileRequest(SocketChannel socketChannel, SelectionKey key) throws Exception{

        // 从请求头中解析文件名称
        Filename filename = getFilename(socketChannel);

        ThreadUtils.println("从网络请求中解析出来文件名：" + filename);
        if (Objects.isNull(filename)){
            return;
        }
        // 从请求中解析文件大小
        Long fileLength = getFileLength(socketChannel);
        ThreadUtils.println("从网络请求中解析出来文件大小：" + fileLength);
        if (Objects.isNull(fileLength)){
            return;
        }
        ThreadUtils.println("从网络请求中解析出来文件长度：" + fileLength);
        // 定义已经读取的文件大小
        Long hasReadFileLength = getHasReadFileLength(socketChannel);
        ThreadUtils.println("初始化已经读取的文件大小：" + hasReadFileLength);

        String client = socketChannel.getRemoteAddress().toString();
        FileOutputStream fileOutputStream = null;
        FileChannel fileChannel = null;
        try {
            fileOutputStream = new FileOutputStream(filename.getAbsoluteFilename());
            fileChannel = fileOutputStream.getChannel();
            // 根据现有文件的大小设置filechannel的position位置
            fileChannel.position(fileChannel.size());

            ByteBuffer byteBuffer = fileByClient.get(client);
            if (Objects.isNull(byteBuffer)){
                byteBuffer = ByteBuffer.allocate(fileLength.intValue());
            }
            int len = socketChannel.read(byteBuffer);
            if (!byteBuffer.hasRemaining()){
                byteBuffer.flip();
                fileChannel.write(byteBuffer);
                cachedRequestMap.get(client).setHasReadFileLength(hasReadFileLength + len);
                fileByClient.remove(client);
            }else {
                cachedRequestMap.get(client).setHasReadFileLength(hasReadFileLength + len);
                fileByClient.put(client, byteBuffer);
                return;
            }
        } finally {
            // 执行到这里说明已经读取完当前socketchannel中的数据（并不代表读取到了完整的图片数据），则关闭 Filechannel 和 FileOutputStream
            fileChannel.close();
            fileOutputStream.close();
        }

        // 说明读取到了完整的数据
        if (hasReadFileLength.equals(fileLength)){
            ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
            socketChannel.write(outBuffer);
            cachedRequestMap.remove(client);
            ThreadUtils.println("文件读取完毕，返回响应给客户端：" + client);

            nameNodeRpcClient.informReplicaReceived(filename.relativeFilename);

            ThreadUtils.println("增量上传文件副本给NameNode节点...");
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }
    }

    /**
     * 发送文件
     * @param socketChannel
     * @param key
     */
    private void handleReadFileRequest(SocketChannel socketChannel, SelectionKey key) throws Exception{
        // 获取文件名称
        Filename filename = getFilename(socketChannel);
        ThreadUtils.println("从网路请求中解析出来文件名称：" + filename);
        if (Objects.isNull(filename)){
            return;
        }
        // 获取文件长度
        File file = new File(filename.getAbsoluteFilename());
        if (!file.exists()){
            ThreadUtils.println("文件不存在：" + filename.getAbsoluteFilename());
            return;
        }
        Long fileLength = file.length();

        FileInputStream fileInputStream = new FileInputStream(filename.getAbsoluteFilename());
        FileChannel fileChannel = fileInputStream.getChannel();

        ByteBuffer byteBuffer = ByteBuffer.allocate(fileLength.intValue() + 8);
        byteBuffer.putLong(fileLength);
        Integer readLen = fileChannel.read(byteBuffer);
        byteBuffer.flip();
        socketChannel.write(byteBuffer);

        fileChannel.close();
        fileInputStream.close();

        if (readLen == fileLength.intValue()){
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }
    }

    /**
     * 返回文件名称
     * @param channel
     * @return
     */
    private Filename getFilename(SocketChannel channel) throws Exception{
        Filename filename = null;
        String client = channel.getRemoteAddress().toString();
        if (cachedRequestMap.containsKey(client)){
            filename = cachedRequestMap.get(client).getFilename();
        }else {
            String relativeFilename = getRelativeFilename(channel);
            if (relativeFilename == null){
                return null;
            }
            // 设置相对路径
            cachedRequestMap.get(client).getFilename().setRelativeFilename(relativeFilename);
            // 获取并设置绝对路径
            String absoluteFilename = getAbsoluteFilename(relativeFilename);
            cachedRequestMap.get(client).getFilename().setAbsoluteFilename(absoluteFilename);
        }
        return filename;
    }

    /**
     * 获取已经读取的文件长度
     * @param channel
     * @return
     * @throws Exception
     */
    private long getHasReadFileLength(SocketChannel channel) throws Exception{
        Long hasReadFileLength = 0L;
        String remoteAddr = channel.getRemoteAddress().toString();
        if (cachedRequestMap.containsKey(remoteAddr)){
            hasReadFileLength = cachedRequestMap.get(remoteAddr).getHasReadFileLength();
        }
        return hasReadFileLength;
    }

    /**
     * 获取文件长度
     * @param socketChannel
     * @return
     * @throws Exception
     */
    private Long getFileLength(SocketChannel socketChannel) throws Exception{
        String client = socketChannel.getRemoteAddress().toString();
        Long fileLength = cachedRequestMap.get(client).getFileLength();
        if (Objects.nonNull(fileLength)){
            return fileLength;
        }
        ByteBuffer byteBuffer = fileLengthByClient.get(client);
        if (Objects.isNull(byteBuffer)){
            byteBuffer = ByteBuffer.allocate(8);
        }
        socketChannel.read(byteBuffer);
        if (!byteBuffer.hasRemaining()){
            byteBuffer.flip();
            fileLength = byteBuffer.getLong();
            fileLengthByClient.remove(client);
        }else {
            fileLengthByClient.put(client,byteBuffer);
        }
        return fileLength;
    }

    /**
     * 获取文件所在磁盘上的绝对路径
     * @param relativeFilename
     * @return
     * @throws Exception
     */
    private String getAbsoluteFilename(String relativeFilename) throws Exception{
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
            throw new RuntimeException("文件路径不能为空：" + dirPath);
        }
        return dirPath + "\\" + relativeFilenameSplited[relativeFilenameSplited.length -1];
    }

    /**
     * 获取相对路径文件名称
     * @param socketChannel
     * @return
     */
    private String getRelativeFilename(SocketChannel socketChannel) throws Exception{
        String client = socketChannel.getRemoteAddress().toString();
        // 从本地缓存中获取相对路径
        String relativeFilename = cachedRequestMap.get(client).getFilename().getRelativeFilename();
        if (StringUtils.isNotBlank(relativeFilename)){
            return relativeFilename;
        }
        ByteBuffer filenameBuffer = filenameByClient.get(client);
        if (Objects.nonNull(filenameBuffer)){
            socketChannel.read(filenameBuffer);
            if (!filenameBuffer.hasRemaining()){
                filenameBuffer.flip();
                relativeFilename = new String(filenameBuffer.array());
                filenameByClient.remove(client);
            }else {
                filenameByClient.put(client,filenameBuffer);
            }
        }else {
            ByteBuffer filenameLengthBuffer = filenameLengthByClient.get(client);
            if (Objects.isNull(filenameLengthBuffer)){
                filenameLengthBuffer = ByteBuffer.allocate(4);
            }
            socketChannel.read(filenameLengthBuffer);
            if (!filenameLengthBuffer.hasRemaining()){
                filenameLengthBuffer.flip();
                int filenameLength = filenameLengthBuffer.getInt();
                filenameLengthByClient.remove(client);
                filenameBuffer = ByteBuffer.allocate(filenameLength);
                socketChannel.read(filenameBuffer);
                if (!filenameBuffer.hasRemaining()){
                    filenameBuffer.flip();
                    relativeFilename = new String(filenameBuffer.array());
                }else {
                    filenameByClient.put(client,filenameBuffer);
                }
            }else {
                filenameLengthByClient.put(client,filenameLengthBuffer);
            }
        }
        return relativeFilename;
    }
    /**
     * 获取缓存的请求
     * @param client
     * @return
     */
    private CachedRequest getCachedRequest(String client){
        CachedRequest cachedRequest = cachedRequestMap.get(client);
        if (Objects.isNull(cachedRequest)){
            cachedRequest = new CachedRequest();
            cachedRequestMap.put(client,cachedRequest);
        }
        return cachedRequest;
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
    class CachedRequest{
        private Integer requestType;
        private Filename filename;
        private Long fileLength;
        private Long hasReadFileLength;

        public CachedRequest() {
        }

        public CachedRequest(Integer requestType, Filename filename, Long fileLength, Long hasReadFileLength) {
            this.requestType = requestType;
            this.filename = filename;
            this.fileLength = fileLength;
            this.hasReadFileLength = hasReadFileLength;
        }

        public Integer getRequestType() {
            return requestType;
        }

        public void setRequestType(Integer requestType) {
            this.requestType = requestType;
        }

        public Filename getFilename() {
            if (Objects.isNull(filename)){
                filename = new Filename();
            }
            return filename;
        }

        public void setFilename(Filename filename) {
            this.filename = filename;
        }

        public Long getFileLength() {
            return fileLength;
        }

        public void setFileLength(Long fileLength) {
            this.fileLength = fileLength;
        }

        public Long getHasReadFileLength() {
            return hasReadFileLength;
        }

        public void setHasReadFileLength(Long hasReadFileLength) {
            this.hasReadFileLength = hasReadFileLength;
        }

        @Override
        public String toString() {
            return "CachedRequest{" +
                    "requestType=" + requestType +
                    ", filename=" + filename +
                    ", fileLength=" + fileLength +
                    ", hasReadFileLength=" + hasReadFileLength +
                    '}';
        }
    }
}
