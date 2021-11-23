package com.lvchao.dfs.datanode.server;

import java.util.ArrayList;
import java.util.List;

/**
 * @Title: StorageInfo
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: DataNodeInfo 存储信息
 * @auther: chao.lv
 * @date: 2021/11/21 20:24
 * @version: V1.0
 */
public class StorageInfo {
    private List<String> filenames = new ArrayList<>();
    private Long storedDataSize = 0L;

    public void addFilename(String filename){
        this.filenames.add(filename);
    }

    public Long addStoredDataSize(Long storedDataSize){
        this.storedDataSize += storedDataSize;
        return storedDataSize;
    }

    public StorageInfo() {
    }

    public StorageInfo(List<String> filenames, Long storedDataSize) {
        this.filenames = filenames;
        this.storedDataSize = storedDataSize;
    }

    public List<String> getFilenames() {
        return filenames;
    }

    public Long getStoredDataSize() {
        return storedDataSize;
    }

    public void setFilenames(List<String> filenames) {
        this.filenames = filenames;
    }

    public void setStoredDataSize(Long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    @Override
    public String toString() {
        return "StorageInfo{" +
                "filenames=" + filenames +
                ", storedDataSize=" + storedDataSize +
                '}';
    }
}
