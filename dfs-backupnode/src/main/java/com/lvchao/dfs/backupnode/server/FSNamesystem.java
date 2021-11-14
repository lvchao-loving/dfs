package com.lvchao.dfs.backupnode.server;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * @Title: FSNamesystem
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: 负责管理元数据的核心组件
 * @auther: chao.lv
 * @date: 2021/10/26 15:06
 * @version: V1.0
 */
public class FSNamesystem {

    private FSDirectory directory;

    /**
     * 记录上一次 checkpint的时间
     */
    private Long checkpointTime = System.currentTimeMillis();
    private String checkpointFile = "";
    private volatile boolean finishedRecover = false;

    public FSNamesystem() {
        this.directory = new FSDirectory();
        // 恢复 BackupNode 内存目录树
        recoverNamespace();
    }

    /**
     * 创建目录
     * @param path 目录路径
     * @return 是否成功
     */
    public Boolean mkdir(Long txid,String path) throws Exception {
        this.directory.mkdir(txid,path);
        return true;
    }

    /**
     * 获取文件目录树的json
     * @return
     * @throws Exception
     */
    public FSImage getFSImage() {
        return directory.getFSImage();
    }

    /**
     * 返回当前同步到的 txid
     * @return
     */
    public Long getSyncedTxid(){
        return directory.getFSImage().getMaxTxid();
    }


    public boolean isFinishedRecover() {
        return finishedRecover;
    }

    public void setFinishedRecover(boolean finishedRecover) {
        this.finishedRecover = finishedRecover;
    }

    /**
     * 恢复 BackupNode 内存目录树
     */
    private void recoverNamespace() {
        finishedRecover = true;
        loadCheckpointInfo();
        loadFSImage();
    }

    public FSDirectory getDirectory() {
        return directory;
    }

    public Long getCheckpointTime() {
        return checkpointTime;
    }

    public String getCheckpointFile() {
        return checkpointFile;
    }

    public void setDirectory(FSDirectory directory) {
        this.directory = directory;
    }

    public void setCheckpointTime(Long checkpointTime) {
        this.checkpointTime = checkpointTime;
    }

    public void setCheckpointFile(String checkpointFile) {
        this.checkpointFile = checkpointFile;
    }

    /**
     * 加载本地 fsimage 文件
     */
    private void loadFSImage() {
        String filePath = "F:\\backupnode\\fsimage-" + directory.getMaxTxid() + ".meta";
        File file = new File(filePath);
        if (!file.exists()){
            ThreadUntils.println("文件不存在：" + filePath);
            return;
        }
        try (
            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
            FileInputStream fileInputStream = new FileInputStream(randomAccessFile.getFD());
            FileChannel channel = fileInputStream.getChannel();
        ){
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            StringBuilder fileContentSB = new StringBuilder();
            while (channel.read(buffer) > 0){
                buffer.flip();
                fileContentSB.append(StandardCharsets.UTF_8.decode(buffer).toString());
                buffer.clear();
            }
            String fileContent = fileContentSB.toString();

            FSDirectory.INode dirTree = JSONObject.parseObject(fileContent, FSDirectory.INode.class);
            directory.setDirTree(dirTree);
        } catch (IOException e) {
            ThreadUntils.println("读取文件内容异常");
            e.printStackTrace();
        }
    }

    /**
     * 加载 checkpointInfo
     */
    private void loadCheckpointInfo() {
        String filePath = "F:\\backupnode\\checkpoint-info.meta";
        File file = new File(filePath);
        if (!file.exists()){
            ThreadUntils.println("文件不存在：" + filePath);
            return;
        }
        try (
            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
            FileInputStream fileInputStream = new FileInputStream(randomAccessFile.getFD());
            FileChannel channel = fileInputStream.getChannel();
        ){
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            StringBuilder fileContentSB = new StringBuilder();
            while (channel.read(buffer) > 0){
                buffer.flip();
                fileContentSB.append(StandardCharsets.UTF_8.decode(buffer).toString());
                buffer.clear();
            }
            String fileContent = fileContentSB.toString();
            Long checkpointTime = Long.valueOf(fileContent.split("_")[0]);
            Long syncedTxid = Long.valueOf(fileContent.split("_")[1]);
            String fsimageFile = String.valueOf(fileContent.split("_")[2]);

            ThreadUntils.println("恢复checkpoint time：" + checkpointTime + ", synced txid: " + syncedTxid + ", fsimage file: " + fsimageFile);

            this.checkpointTime = checkpointTime;
            this.directory.setMaxTxid(syncedTxid);
            this.checkpointFile = fsimageFile;

            directory.setMaxTxid(syncedTxid);
        } catch (IOException e) {
            ThreadUntils.println("读取文件内容异常");
            e.printStackTrace();
        }
    }
}
