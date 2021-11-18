# DataNode节点
## 主要组件
DataNode节点创建了两个组件【NameNodeOfferService】和【DataNodeNIOServer】

### 【NameNodeOfferService】组件
NameNodeOfferService作用：负责和NameNode节点通信，主要是注册和发送心跳
NameNodeOfferService实现：创建了NameNodeServiceActor组件，触发对NameNode节点发送注册和心跳的功能

#### NameNodeServiceActor组件
NameNodeServiceActor组件中创建了两个组件【RegisterThread】和【HeartbeatThread】完成对客户端注册和心跳的发送的功能

### 【DataNodeNIOServer】组件
DataNodeNIOServer作用：接收发送图片的请求
DataNodeNIOServer实现：底层使用NIO和队列完成

