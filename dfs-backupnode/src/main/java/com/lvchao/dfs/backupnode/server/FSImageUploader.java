package com.lvchao.dfs.backupnode.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * @Title: FSImageUploader
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/10/31 19:54
 * @version: V1.0
 */
public class FSImageUploader extends Thread{

    private FSImage fsImage;

    public FSImageUploader(FSImage fsImage){
        this.fsImage = fsImage;
    }


    @Override
    public void run() {
        SocketChannel socketChannel = null;
        Selector selector = null;
        try{
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress("localhost",9000));

            selector = Selector.open();
            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            boolean uploading = true;

            while(uploading){
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()){
                    SelectionKey selectionKey = keysIterator.next();
                    keysIterator.remove();
                    if (selectionKey.isConnectable()){
                        socketChannel = (SocketChannel) selectionKey.channel();

                        if (socketChannel.isConnectionPending()){
                            socketChannel.finishConnect();
                            ByteBuffer byteBuffer = ByteBuffer.wrap(fsImage.getFsimageJson().getBytes());
                            socketChannel.write(byteBuffer);
                        }
                        socketChannel.register(selector,SelectionKey.OP_READ);

                    }else if(selectionKey.isReadable()){
                        ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
                        socketChannel = (SocketChannel) selectionKey.channel();
                        int count = socketChannel.read(byteBuffer);

                        if (count > 0){
                            ThreadUntils.println("上传fsimage文件成功，响应消息为：" + new String(byteBuffer.array(), 0, count));
                            socketChannel.close();
                            uploading = false;
                        }
                    }
                }
            }

        }catch (Exception exception){
            // TODO 将异常同步到 BackupNode节点
            ThreadUntils.println("FSImageUploader 线程发送fsimage文件异常");
        }finally {
            if (socketChannel != null){
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (selector != null){
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
