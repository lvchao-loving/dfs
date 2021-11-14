package com.lvchao.dfs.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Title: Test01
 * @Package: com.lvchao.dfs.client
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/14 15:03
 * @version: V1.0
 */
public class Test01 {
    public static AtomicInteger atomicInteger = new AtomicInteger(1);

    public static void main(String[] args) {
        System.out.println(atomicInteger.getAndIncrement());
        System.out.println(atomicInteger.incrementAndGet());
    }
}
