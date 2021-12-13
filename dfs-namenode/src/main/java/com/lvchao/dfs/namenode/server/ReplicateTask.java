package com.lvchao.dfs.namenode.server;

/**
 * @Title: ReplicateTask
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 副本复制任务
 * @auther: chao.lv
 * @date: 2021/12/4 21:39
 * @version: V1.0
 */
public class ReplicateTask {
    private String filename;
    private Long fileLength;
    private DataNodeInfo  sourceDataNodeInfo;
    private DataNodeInfo  destDataNodeInfo;

    public ReplicateTask() {
    }

    public ReplicateTask(String filename, Long fileLength, DataNodeInfo sourceDataNodeInfo, DataNodeInfo destDataNodeInfo) {
        this.filename = filename;
        this.fileLength = fileLength;
        this.sourceDataNodeInfo = sourceDataNodeInfo;
        this.destDataNodeInfo = destDataNodeInfo;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Long getFileLength() {
        return fileLength;
    }

    public void setFileLength(Long fileLength) {
        this.fileLength = fileLength;
    }

    public DataNodeInfo getSourceDataNodeInfo() {
        return sourceDataNodeInfo;
    }

    public void setSourceDataNodeInfo(DataNodeInfo sourceDataNodeInfo) {
        this.sourceDataNodeInfo = sourceDataNodeInfo;
    }

    public DataNodeInfo getDestDataNodeInfo() {
        return destDataNodeInfo;
    }

    public void setDestDataNodeInfo(DataNodeInfo destDataNodeInfo) {
        this.destDataNodeInfo = destDataNodeInfo;
    }

    @Override
    public String toString() {
        return "ReplicateTask{" +
                "filename='" + filename + '\'' +
                ", fileLength=" + fileLength +
                ", sourceDataNodeInfo=" + sourceDataNodeInfo +
                ", destDataNodeInfo=" + destDataNodeInfo +
                '}';
    }
}
