package com.lvchao.dfs.backupnode.server;

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

    public FSNamesystem() {
        this.directory = new FSDirectory();
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
}
