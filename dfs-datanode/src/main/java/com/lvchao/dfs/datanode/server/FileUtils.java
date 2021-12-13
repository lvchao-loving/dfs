package com.lvchao.dfs.datanode.server;

import org.apache.commons.lang3.StringUtils;

import java.io.File;

/**
 * @Title: FileUtils
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: 文件工具类
 * @auther: chao.lv
 * @date: 2021/12/5 16:13
 * @version: V1.0
 */
public class FileUtils {

    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    /**
     * 获取文件所在磁盘上的绝对路径
     * @param relativeFilename
     * @return
     * @throws Exception
     */
    public String getAbsoluteFilename(String relativeFilename) throws Exception{
        String[] relativeFilenameSplited = relativeFilename.split("/");
        String dirPath = dataNodeConfig.DATA_DIR;
        for (int i = 0; i < relativeFilenameSplited.length - 1; i++) {
            if (StringUtils.isBlank(relativeFilenameSplited[i])){
                continue;
            }
            dirPath += "\\" + relativeFilenameSplited[i];
        }
        // 判断文件路径是否存在不存在则创建
        File dir = new File(dirPath);
        if (!dir.exists()){
            dir.mkdirs();
            // throw new RuntimeException("文件路径不能为空：" + dirPath);
        }
        return dirPath + "\\" + relativeFilenameSplited[relativeFilenameSplited.length -1];
    }
}
