package com.lvchao.dfs.backupnode.server;

/**
 * @Title: BackupNode
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: 负责同步editslog的进程
 * @auther: chao.lv
 * @date: 2021/10/26 14:30
 * @version: V1.0
 */
public class BackupNode {
    private volatile Boolean isRunning = true;

    private FSNamesystem fsNamesystem;
    /**
     * rpc client
     */
    private NameNodeRpcClient nameNode;

    public static void main(String[] args) throws Exception {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
    }

    public void init(){
        this.fsNamesystem = new FSNamesystem();
        this.nameNode = new NameNodeRpcClient();
    }

    public void start() throws Exception {
        // 拉取 NameNode 节点的数据，同步到 BackupNode节点中，并且文件目录树存储在内存中
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(this, fsNamesystem, nameNode);
        editsLogFetcher.setName("EditsLogFetcher");
        editsLogFetcher.start();

        FSImageCheckpointer fsImageCheckpointer = new FSImageCheckpointer(this, fsNamesystem, nameNode);
        fsImageCheckpointer.setName("FSImageCheckpointer");
        fsImageCheckpointer.start();
    }

    public void run() throws Exception {
        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    public Boolean isRunning() {
        return isRunning;
    }
}
