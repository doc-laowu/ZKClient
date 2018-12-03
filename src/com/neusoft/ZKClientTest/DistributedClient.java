package com.neusoft.ZKClientTest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedClient {

    private static final String connectString = "node01:2181,node02:2181,node03:2181";
    private static final int sessionTimeOut = 2000;
    private static final String group = "/servers/";

    private ZooKeeper zkClient = null;

    private volatile List<String> serverList = null;

    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                try {
                    //重新更新服务器列表
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    //获取服务器子节点信息
    public void getServerList() throws KeeperException, InterruptedException {
        List<String> children =  zkClient.getChildren(group, true);
        serverList =  new ArrayList<String>();
        for (String child : children)
        {
            byte[] data = zkClient.getData(group+child, false, null);
            serverList.add(new String(data));
        }

        for (String child : serverList)
        {
            System.out.println(child);
        }
    }

    public void handleBussiness() throws InterruptedException, KeeperException {
        System.out.println("Client is working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {

            //获取zk的连接
            DistributedClient client = new DistributedClient();
            //获取serverce的字节信息，获取服务器列表信息
            client.getServerList();
            //启动业务线程
            client.handleBussiness();
    }
}