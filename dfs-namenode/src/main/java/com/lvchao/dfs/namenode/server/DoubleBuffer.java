package com.lvchao.dfs.namenode.server;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @Title: DoubleBuffer
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 内存双缓冲
 * @auther: chao.lv
 * @date: 2021/10/25 13:37
 * @version: V1.0
 */
public class DoubleBuffer {
    /**
     * 单块editslog缓冲区的最大大小：默认是512字节
     */
    public static final Integer EDIT_LOG_BUFFER_LIMIT = 25 * 1024;

    /**
     * 是专门用来承载线程写入edits log
     */
    private EditLogBuffer currentBuffer = new EditLogBuffer();
    /**
     * 专门用来将数据同步到磁盘中去的一块缓冲
     */
    private EditLogBuffer syncBuffer = new EditLogBuffer();
    /**
     * 已经输入磁盘中的txid
     */
    private List<String> flushedTxids = new CopyOnWriteArrayList<>();

    public List<String> getFlushedTxids() {
        return flushedTxids;
    }

    /**
     * 将edits log写到内存缓冲里去
     * @param log
     */
    public void write(EditLog log) throws IOException {
        currentBuffer.write(log);
    }

    /**
     * 判断一下当前的缓冲区是否写满了需要刷到磁盘上去
     * @return
     */
    public boolean shouldSyncToDisk() {
        if(currentBuffer.size() >= EDIT_LOG_BUFFER_LIMIT) {
            return true;
        }
        return false;
    }

    /**
     * 交换两块缓冲区，为了同步内存数据到磁盘做准备
     */
    public void setReadyToSync() {
        EditLogBuffer tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    /**
     * 将syncBuffer缓冲区中的数据刷入磁盘中
     */
    public void flush() throws IOException {
        syncBuffer.flush();
        syncBuffer.clear();
    }

    /**
     * 获取当前缓冲区里的数据
     * @return
     */
    public String[] getBufferedEditsLog(){
        if(currentBuffer.size() == 0){
            return null;
        }
        String editsLogRawData = new String(currentBuffer.getBufferData());
        return editsLogRawData.split("\n");
    }

    class EditLogBuffer {
        /**
         * 单块editslog缓冲区的最大大小：默认是512kb
         */
        public final Integer EDIT_LOG_BUFFER_LIMIT = DoubleBuffer.EDIT_LOG_BUFFER_LIMIT;

        private ByteArrayOutputStream buffer;
        private volatile Long startTxid = -1L;
        private volatile Long endTxid = -1L;


        public EditLogBuffer(){
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
            // 将流中的数据输出并转成字节数组
            byte[] data = buffer.toByteArray();
            // 将字节数组装成 byteBuffer
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);

            if (startTxid.longValue() == endTxid.longValue()){
                ThreadUntils.println("无数据，不需要持久化磁盘");
                return;
            }
            // 拼接文件路径
            String editsLogFilePath = "F:\\editslog\\edits-" + startTxid + "-" + endTxid + ".log";
            // 将持久化磁盘的数据记录在 doublebuffer 中
            flushedTxids.add(startTxid + "_" + endTxid);


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

            //return new EditslogInfo(startTxid, endTxid, editsLogFilePath);
        }

        /**
         * 清空掉内存缓冲区里面的数据
         */
        public void clear() {
            buffer.reset();
            startTxid = -1L;
            endTxid = -1L;
        }

        /**
         * 获取内存缓冲区当前的数据
         * @return
         */
        public byte[] getBufferData(){
            return buffer.toByteArray();
        }
    }
}
