package com.lvchao.dfs.namenode.server;

/**
 * @Title: EditLogFactory
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 生成 editlog 内容的工厂类
 * @auther: chao.lv
 * @date: 2021/11/19 7:25
 * @version: V1.0
 */
public class EditLogFactory {

    public static String mkdir(String path) {
        return "{'OP':'MKDIR','PATH':'" + path + "'}";
    }

    public static String create(String path) {
        return "{'OP':'CREATE','PATH':'" + path + "'}";
    }
}
