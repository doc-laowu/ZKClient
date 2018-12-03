package com.neusoft.ZKClientTest;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributedServer {

    private static final String connectString = "node01:2181,node02:2181,node03:2181";
    private static final int sessionTimeOut = 2000;
    private static final String group = "/servers/";

    private ZooKeeper zkClient = null;

    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getType()+"----"+event.getPath());

                try {
                    zkClient.getChildren(group, true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void registerServer(String hostname) throws KeeperException, InterruptedException {
        String create = zkClient.create(group+"server", hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"is online"+create);
    }

    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname+"is working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        //获取zk连接

        DistributedServer server = new DistributedServer();
        server.getConnect();
        //利用zk进行消息注册

        server.registerServer(args[0]);
        //启动业务功能
        server.handleBussiness(args[0]);

    }
}
