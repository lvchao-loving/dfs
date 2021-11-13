package com.lvchao.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;

/**
 * @Title: EditsLogFetcher
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: 从namenode同步editslog的组件
 * @auther: chao.lv
 * @date: 2021/10/26 15:03
 * @version: V1.0
 */
public class EditsLogFetcher extends Thread{

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static final Integer BACKUP_NODE_FETCH_SIZE = 10;

    /**
     * BackupNode 作用就是
     */
    private BackupNode backupNode;
    private NameNodeRpcClient namenode;
    private FSNamesystem fsNamesystem;

    public EditsLogFetcher(BackupNode backupNode,FSNamesystem fsNamesystem,NameNodeRpcClient namenode) {
        this.backupNode = backupNode;
        this.namenode = namenode;
        this.fsNamesystem = fsNamesystem;
    }

    @Override
    public void run() {

        ThreadUntils.println("EditsLogFetcher抓取线程已经启动...");

        while(backupNode.isRunning()) {
            try {
                JSONArray editsLogs = namenode.fetchEditsLog();

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
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
