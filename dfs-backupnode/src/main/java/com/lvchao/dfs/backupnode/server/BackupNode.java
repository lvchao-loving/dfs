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
    /**
     * BackupNode 主程序启动类
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
    }

    /**
     * 标识 BackupNode 的运行状况
     */
    private volatile boolean running = true;

    /**
     * 元数据管理
     */
    private FSNamesystem fsNamesystem;

    /**
     * rpc client
     */
    private NameNodeRpcClient nameNode;

    public void setFsNamesystem(FSNamesystem fsNamesystem) {
        this.fsNamesystem = fsNamesystem;
    }

    public void setNameNode(NameNodeRpcClient nameNode) {
        this.nameNode = nameNode;
    }

    public FSNamesystem getFsNamesystem() {
        return fsNamesystem;
    }

    public NameNodeRpcClient getNameNode() {
        return nameNode;
    }

    public void init(){
        // 创建元数据时，会结合之间的本地元数据文件情况进行【元数据恢复】
        this.fsNamesystem = new FSNamesystem();
        this.nameNode = new NameNodeRpcClient();
    }

    public void start() throws Exception {
        // 拉取 NameNode 节点的数据，同步到 BackupNode节点中，并且文件目录树存储在内存中
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(this);
        editsLogFetcher.setName("EditsLogFetcher");
        editsLogFetcher.start();

        FSImageCheckpointer fsImageCheckpointer = new FSImageCheckpointer(this);
        fsImageCheckpointer.setName("FSImageCheckpointer");
        fsImageCheckpointer.start();
    }

    public void run() throws Exception {
        while (isRunning()) {
            Thread.sleep(1000);
        }
    }

    public boolean isRunning() {
        return running;
    }
}
