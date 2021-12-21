package com.lvchao.dfs.namenode.server;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @Title: DataNodeInfo
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: datanode节点信息
 * @auther: chao.lv
 * @date: 2021/11/19 6:49
 * @version: V1.0
 */
public class DataNodeInfo {
    /**
     * 机器ip
     */
    private String ip;
    /**
     * 机器名称
     */
    private String hostname;
    /**
     * nio 通信端口
     */
    private Integer nioPort;
    /**
     * 最近一次心跳的时间
     */
    private Long latestHeartbeatTime;
    /**
     * 已经存储数据的大小
     */
    private Long storedDataSize;
    /**
     * 副本复制任务队列
     */
    private ConcurrentLinkedQueue<ReplicateTask> replicateTaskQueue = new ConcurrentLinkedQueue<>();
    /**
     * 删除副本任务
     */
    private ConcurrentLinkedQueue<RemoveReplicaTask> removeReplicaTaskQueue = new ConcurrentLinkedQueue<>();

    public DataNodeInfo() {
    }

    public DataNodeInfo(String ip, String hostname, Integer nioPort) {
        this.ip = ip;
        this.hostname = hostname;
        this.nioPort = nioPort;
        this.latestHeartbeatTime = System.currentTimeMillis();
        this.storedDataSize = 0L;
    }

    public String getId(){
        return ip + "_" + hostname;
    }

    public void addReplicateTask(ReplicateTask replicateTask){
        replicateTaskQueue.offer(replicateTask);
    }

    public ReplicateTask pollReplicateTask(){
        if (!replicateTaskQueue.isEmpty()){
            return replicateTaskQueue.poll();
        }
        return null;
    }

    public void addRemoveReplicaTask(RemoveReplicaTask removeReplicaTask){
        removeReplicaTaskQueue.offer(removeReplicaTask);
    }

    public RemoveReplicaTask pollRemoveReplicaTask(){
        if (!removeReplicaTaskQueue.isEmpty()){
            return removeReplicaTaskQueue.poll();
        }
        return null;
    }

    public String getIp() {
        return ip;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getNioPort() {
        return nioPort;
    }

    public Long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public Long getStoredDataSize() {
        return storedDataSize;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setNioPort(Integer nioPort) {
        this.nioPort = nioPort;
    }

    public void setLatestHeartbeatTime(Long latestHeartbeatTime) {
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public void setStoredDataSize(Long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    public void addStoredDataSize(Long storedDataSize){
        this.storedDataSize += storedDataSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) { return false;}
        DataNodeInfo that = (DataNodeInfo) o;
        return Objects.equals(ip, that.ip) && Objects.equals(hostname, that.hostname) && Objects.equals(nioPort, that.nioPort) && Objects.equals(latestHeartbeatTime, that.latestHeartbeatTime) && Objects.equals(storedDataSize, that.storedDataSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, hostname, nioPort, latestHeartbeatTime, storedDataSize);
    }

    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "ip='" + ip + '\'' +
                ", hostname='" + hostname + '\'' +
                ", nioPort=" + nioPort +
                ", latestHeartbeatTime=" + latestHeartbeatTime +
                ", storedDataSize=" + storedDataSize +
                '}';
    }
}
