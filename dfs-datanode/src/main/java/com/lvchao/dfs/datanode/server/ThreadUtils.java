package com.lvchao.dfs.datanode.server;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Title: ThreadUntils
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: 线程打印工具类
 * @auther: chao.lv
 * @date: 2021/11/13 12:47
 * @version: V1.0
 */
public class ThreadUtils {

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void println(String data){
        System.out.println("【" + simpleDateFormat.format(new Date())+ "   " + Thread.currentThread().getName() + "】" + data);
    }
}
