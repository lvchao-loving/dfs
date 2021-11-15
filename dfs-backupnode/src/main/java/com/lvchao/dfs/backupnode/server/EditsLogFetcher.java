package com.lvchao.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @Title: EditsLogFetcher
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: 从namenode同步editslog的组件
 * @auther: chao.lv
 * @date: 2021/10/26 15:03
 * @version: V1.0
 */
public class EditsLogFetcher extends Thread{

    public static final Integer BACKUP_NODE_FETCH_SIZE = 10;

    private BackupNode backupNode;
    private NameNodeRpcClient namenode;
    private FSNamesystem fsNamesystem;

    public EditsLogFetcher(BackupNode backupNode) {
        this.backupNode = backupNode;
        this.namenode = backupNode.getNameNode();
        this.fsNamesystem = backupNode.getFsNamesystem();
    }

    @Override
    public void run() {

        ThreadUntils.println("EditsLogFetcher 线程已经启动...");

        while(backupNode.isRunning()) {
            // 临时记录一下执行时间
            Long now = System.currentTimeMillis();
            try {
                // 判断元数据是否已经恢复
                if (!fsNamesystem.isFinishedRecover()){
                    ThreadUntils.println("当前还没完成元数据恢复，不进行editlog同步......，等待5秒钟");
                    Thread.sleep(5000);
                    continue;
                }

                JSONArray editsLogs = namenode.fetchEditsLog(fsNamesystem.getSyncedTxid());
                // 正常成功拉取数据则设置成 true
                namenode.setNamenodeRunning(true);
                if (editsLogs.size() == 0){
                    ThreadUntils.println("没有拉取到任何一条editslog，等待5秒后继续尝试拉取");
                    Thread.sleep(5000);
                    continue;
                }

                if (editsLogs.size() < BACKUP_NODE_FETCH_SIZE){
                    ThreadUntils.println("拉取到editslog 不足10条，等待5秒后继续尝试拉取");
                    Thread.sleep(5000);
                }

                for(int i = 0; i < editsLogs.size(); i++) {
                    JSONObject editsLog = editsLogs.getJSONObject(i);
                    ThreadUntils.println("拉取到一条editslog：" + editsLog.toJSONString());
                    String op = editsLog.getString("OP");

                    if(op.equals("MKDIR")) {
                        String path = editsLog.getString("PATH");
                        try {
                            fsNamesystem.mkdir(editsLog.getLongValue("txid"), path);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

            } catch (Exception e) {
                // 从 Namenode 节点中拉取数据出现了异常，则判断 Namenode 运行异常
                namenode.setNamenodeRunning(false);
                // TODO
                ThreadUntils.println("fetch edits log 异常:" + e.getMessage() + "【后续优化】，目前睡眠1秒钟后重试...执行时间" + (System.currentTimeMillis() - now));
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
    }
}
