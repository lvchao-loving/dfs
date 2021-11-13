package com.lvchao.dfs.namenode.server;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * editslog缓冲区
 */
public class EditLogBuffer2 {
    /**
     * 单块editslog缓冲区的最大大小：默认是512kb
     */
    public static final Integer EDIT_LOG_BUFFER_LIMIT = DoubleBuffer.EDIT_LOG_BUFFER_LIMIT;

    private ByteArrayOutputStream buffer;
    private volatile Long startTxid = -1L;
    private volatile Long endTxid = -1L;


    public EditLogBuffer2(){
        this.buffer = new ByteArrayOutputStream( EDIT_LOG_BUFFER_LIMIT * 2 );
    }
    /**
     * 将editslog日志写入缓冲区
     *
     * @param log
     */
    public void write(EditLog log) throws IOException {
        // 说明是系统第一次启动
        if (startTxid == -1){
            startTxid = log.getTxid();
        }
        this.endTxid = log.getTxid();
        buffer.write(log.getContent().getBytes());
        buffer.write("\n".getBytes());
        ThreadUntils.println("写入一条editslog：" + log.getContent() + "，当前系统缓冲区的大小是：" + size());
    }

    /**
     * 获取当前缓冲区已经写入数据的字节数量
     *
     * @return
     */
    public Integer size() {
        return buffer.size();
    }

    /**
     * 刷磁盘
     * @throws IOException
     */
    public void flush() throws IOException {
       // ThreadUntils.println("执行刷盘操作，当前大小:" + size());
        byte[] data = buffer.toByteArray();
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        String editsLogFilePath = "F:\\editslog\\edits-"
                + startTxid + "-" + endTxid + ".log";

        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel editsLogFileChannel = null;
        try {
            // 读写模式，数据写入缓冲区中
            file = new RandomAccessFile(editsLogFilePath, "rw");
            out = new FileOutputStream(file.getFD());
            editsLogFileChannel = out.getChannel();

            editsLogFileChannel.write(dataBuffer);
            // 强制把数据刷入磁盘上
            editsLogFileChannel.force(false);
        }  finally {
            if(out != null) {
                out.close();
            }
            if(file != null) {
                file.close();
            }
            if(editsLogFileChannel != null) {
                editsLogFileChannel.close();
            }
        }
        this.startTxid = endTxid + 1;
    }

    /**
     * 清空掉内存缓冲区里面的数据
     */
    public void clear() {
        buffer.reset();
        startTxid = -1L;
        endTxid = -1L;
    }

}