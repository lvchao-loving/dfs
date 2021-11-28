package com.lvchao.dfs.client;

/**
 * @Title: FileSystem
 * @Package: com.lvchao.dfs.client
 * @Description: 作为文件系统的接口
 * @auther: chao.lv
 * @date: 2021/10/22 21:08
 * @version: V1.0
 */
public interface FileSystem {
    /**
     * 创建目录
     * @param path
     * @throws Exception
     */
    void mkdir(String path) throws Exception;

    /**
     * 关闭连接
     * @throws Exception
     */
    void shutdown() throws Exception;

    /**
     * 上传文件
     * @param file 文件的字节数组
     * @param filename 文件名称
     * @param fileSize 文件大小
     * @throws Exception
     */
    Boolean upload(byte[] file, String filename,Long fileSize) throws Exception;

    /**
     * 下载文件
     * @param filename 文件名称
     * @return 文件的字节数组
     * @throws Exception
     */
    byte[] download(String filename) throws Exception;
}
