package com.lvchao.dfs.namenode.server;

/**
 * @Title: RemoveReplicaTask
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 删除副本任务
 * @auther: chao.lv
 * @date: 2021/12/5 16:44
 * @version: V1.0
 */
public class RemoveReplicaTask {
    private String filename;
    private DataNodeInfo dataNodeInfo;

    public RemoveReplicaTask() {
    }

    public RemoveReplicaTask(String filename, DataNodeInfo dataNodeInfo) {
        this.filename = filename;
        this.dataNodeInfo = dataNodeInfo;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public DataNodeInfo getDataNodeInfo() {
        return dataNodeInfo;
    }

    public void setDataNodeInfo(DataNodeInfo dataNodeInfo) {
        this.dataNodeInfo = dataNodeInfo;
    }

    @Override
    public String toString() {
        return "RemoveReplicaTask{" +
                "filename='" + filename + '\'' +
                ", dataNodeInfo=" + dataNodeInfo +
                '}';
    }
}
