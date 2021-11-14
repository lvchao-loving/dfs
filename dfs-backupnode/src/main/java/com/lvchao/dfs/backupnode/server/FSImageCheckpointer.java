package com.lvchao.dfs.backupnode.server;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * @Title: FSImageCheckpointer
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: fsimage文件的checkpoint组件
 * @auther: chao.lv
 * @date: 2021/10/30 19:42
 * @version: V1.0
 */
public class FSImageCheckpointer extends Thread{

    /**
     * checkpoint 操作的时间间隔
     */
    // public static final Integer CHECKPOINT_INTERVAL = 1 * 60 * 60 * 1000;
    public static final Integer CHECKPOINT_INTERVAL = 2 * 30 * 1000;

    private BackupNode backupNode;

    private FSNamesystem fsNamesystem;

    private NameNodeRpcClient nameNode;

    private String lastFsimageFile = "";

    public FSImageCheckpointer(BackupNode backupNode, FSNamesystem fsNamesystem, NameNodeRpcClient nameNode){
        this.backupNode = backupNode;
        this.fsNamesystem = fsNamesystem;
        this.nameNode = nameNode;
    }

    @Override
    public void run() {
        ThreadUntils.println("fsimage checkpoint定时调度线程启动...");
        while (backupNode.isRunning()){
            try {
                TimeUnit.MILLISECONDS.sleep(CHECKPOINT_INTERVAL);
                ThreadUntils.println("准备执行checkpoint操作，写入 fsimage 文件...");
                doCheckpoint();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将 fsimage 持久化到磁盘中
     * @param
     */
    private void doCheckpoint() throws Exception{
        // 获取内存中的目录树
        FSImage fsImage = fsNamesystem.getFSImage();

        // 删除BackupNode节点中上一次本地FsimageFile文件
        removeLastFsimageFile();

        // 把传递过来的文件写入班底磁盘
        writeFSImageFile(fsImage);

        // 把 FsimageFile 文件发送给 NameNode 节点
        uploadFSImageFile(fsImage);

        // 将 BackupNode 节点同步到的 Txid 发送给 NameNode 节点
        updateCheckpointTxid(fsImage);
    }

    /**
     * 更新 checkpointTxid
     * @param fsImage
     */
    private void updateCheckpointTxid(FSImage fsImage) {
        nameNode.updateCheckpointTxid(fsImage.getMaxTxid());
    }

    /**
     * 上传fsimage文件
     * @param fsImage
     */
    private void uploadFSImageFile(FSImage fsImage) {
        FSImageUploader fsImageUploader = new FSImageUploader(fsImage);
        fsImageUploader.setName("FSImageUploader");
        fsImageUploader.start();
    }

    /**
     * 写入最新的fsimage文件
     * @param fsImage
     */
    private void writeFSImageFile(FSImage fsImage) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFsimageJson().getBytes());

        String fsimageFilePath = "F:\\backupnode\\fsimage-" + fsImage.getMaxTxid() + ".meta";
        lastFsimageFile = fsimageFilePath;

        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel channel = null;

        try {
            file = new RandomAccessFile(fsimageFilePath, "rw");
            out = new FileOutputStream(file.getFD());
            channel = out.getChannel();

            channel.write(buffer);
            channel.force(false);
            ThreadUntils.println("将内存目录树同步到本地磁盘中path=" + fsimageFilePath);
        } finally {
            if(out != null) {
                out.close();
            }
            if(file != null) {
                file.close();
            }
            if(channel != null) {
                channel.close();
            }
        }
    }

    /**
     * 删除上一个fsimage磁盘文件
     */
    public void removeLastFsimageFile(){
        if (StringUtils.isBlank(lastFsimageFile)){
            ThreadUntils.println("上一个 FsimageFile文件不存在");
            return;
        }
        ThreadUntils.println("删除 FsimageFile文件不存在");
        File file = new File(lastFsimageFile);
        if (file.exists()){
            file.delete();
        }
    }
}
