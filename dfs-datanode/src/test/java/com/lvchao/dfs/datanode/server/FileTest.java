package com.lvchao.dfs.datanode.server;

import java.io.File;

/**
 * @Title: FileTest
 * @Package: com.lvchao.dfs.datanode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/20 17:53
 * @version: V1.0
 */
public class FileTest {
    public static void main(String[] args) {
        String filePath = "F:\\tmp1\\image\\product\\iphone001.jpg";
        File file = new File(filePath);
        if (file.exists()){
            System.out.println("文件存在");
        }else {
            System.out.println("文件不存在");
        }
    }
}
