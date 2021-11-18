package com.lvchao.dfs.namenode.server;

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

    public DataNodeInfo() {
    }

    public DataNodeInfo(String ip, String hostname, Integer nioPort) {
        this.ip = ip;
        this.hostname = hostname;
        this.nioPort = nioPort;
        this.latestHeartbeatTime = System.currentTimeMillis();
        this.storedDataSize = 0L;
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
