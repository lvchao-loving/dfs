package com.lvchao.dfs.datanode.server;

/**
 * @Title: OperatorTest
 * @Package: com.lvchao.dfs.datanode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/12/12 15:38
 * @version: V1.0
 */
public class OperatorTest {
    public static void main(String[] args) {
        Integer a4 = 1<<2;
        Integer a8 = 1<<3;
        System.out.println(a4);
        System.out.println(a8);
        System.out.println(a4&~5);
    }
}
