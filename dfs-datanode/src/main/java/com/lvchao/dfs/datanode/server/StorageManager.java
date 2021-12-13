package com.lvchao.dfs.datanode.server;

import java.io.File;

/**
 * @Title: StorageManager
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: 磁盘存储管理组件
 * @auther: chao.lv
 * @date: 2021/11/24 15:13
 * @version: V1.0
 */
public class StorageManager {
    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    /**
     * 获取 DataNode 节点存储信息
     * @return
     */
    public StorageInfo getDataNodeStoredInfo() {
        StorageInfo storageInfo = new StorageInfo();

        File fileDir = new File(dataNodeConfig.DATA_DIR);

        scanFils(fileDir,storageInfo);

        return storageInfo;
    }

    /**
     * 扫描遍历所有文件
     * @param fileDir
     * @param storageInfo
     */
    private void scanFils(File fileDir, StorageInfo storageInfo){
        File[] fileList = fileDir.listFiles();
        for (File file:fileList) {
            if (file.isFile()){
                storageInfo.addStoredDataSize(file.length());
                storageInfo.addFilename(file.getPath().replace(dataNodeConfig.DATA_DIR,"").replace("\\","/") + "_" + file.length());
            }else {
                scanFils(file,storageInfo);
            }
        }
    }
}
