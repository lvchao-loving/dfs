package com.lvchao.dfs.datanode.server;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Title: ReplicateManager
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: 副本复制管理组件
 * @auther: chao.lv
 * @date: 2021/12/5 14:49
 * @version: V1.0
 */
public class ReplicateManager {
    public static final Integer REPLICATE_THREAD_NUM = 3;

    private NIOClient nioClient = new NIOClient();

    private ConcurrentLinkedQueue<JSONObject> replicateTaskQueue = new ConcurrentLinkedQueue<>();

    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    private NameNodeRpcClient namenodeRpcClient;

    public ReplicateManager(NameNodeRpcClient namenodeRpcClient){
        this.namenodeRpcClient = namenodeRpcClient;
        for (int i = 0; i < REPLICATE_THREAD_NUM; i++) {
            ReplicateWorker replicateWorker = new ReplicateWorker();
            replicateWorker.setName("ReplicateWorker" + i);
            replicateWorker.start();
        }
    }

    /**
     * 添加复制任务
     * @param replicateTask
     */
    public void addReplicateTask(JSONObject replicateTask){
        replicateTaskQueue.offer(replicateTask);
    }

    /**
     * 副本复制线程
     */
    class ReplicateWorker extends Thread {
        @Override
        public void run() {
            while (true){
                FileOutputStream fileOutputStream = null;
                FileChannel fileChannel = null;
                try {
                    JSONObject replicateTask = replicateTaskQueue.poll();
                    if (replicateTask == null){
                        TimeUnit.SECONDS.sleep(1);
                        continue;
                    }
                    String filename = replicateTask.getString("filename");
                    Long fileLength = replicateTask.getLong("fileLength");
                    JSONObject sourceDatanode = replicateTask.getJSONObject("sourceDataNodeInfo");
                    String hostname = sourceDatanode.getString("hostname");
                    Integer nioPort = sourceDatanode.getInteger("nioPort");

                    byte[] fileByteArray = nioClient.readFile(hostname, nioPort, filename);
                    ByteBuffer fileBuffer = ByteBuffer.wrap(fileByteArray);

                    String absoluteFilename = getAbsoluteFilename(filename);

                    fileOutputStream = new FileOutputStream(absoluteFilename);
                    fileChannel = fileOutputStream.getChannel();

                    fileChannel.write(fileBuffer);

                    namenodeRpcClient.informReplicaReceived(filename + "_" + fileLength);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if(fileChannel != null) {
                            fileChannel.close();
                        }
                        if(fileOutputStream != null) {
                            fileOutputStream.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
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
            dir.mkdirs();
            // throw new RuntimeException("文件路径不能为空：" + dirPath);
        }
        return dirPath + "\\" + relativeFilenameSplited[relativeFilenameSplited.length -1];
    }
}
