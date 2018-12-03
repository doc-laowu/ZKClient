package com.neusoft.ZKClientTest;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZooKeeperClient {

    private static final String connectString = "node01:2181,node02:2181,node03:2181";
    private static final int sessionTimeOut = 2000;

    private ZooKeeper zkClient = null;

    @Before
    public void Init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                //收到事件之后的回调函数（我们自己的事件处理逻辑）
                System.out.println(event.getType()+"---:---"+event.getPath());
                try {
                    //再次去监听
                    zkClient.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //创建数据节点到ZooKeeper种
    @Test
    public void CreateNodeData() throws KeeperException, InterruptedException {

        String createNode = zkClient.create("/idea", "hello Zk".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //上传的数据可以是任何类型，但是需要转成字节数组
    }


    //判断某个节点数据是否存在c
    @Test
    public void TestExist() throws KeeperException, InterruptedException {

        Stat stat =  zkClient.exists("/", true);
        System.out.print(stat==null?"不存在":"存在");
    }

    //获取子节点
    @Test
    public void getChild() throws KeeperException, InterruptedException {

        List<String> children = zkClient.getChildren("/", true);
        for (String child: children)
        {
            System.out.println(child);
        }
    }

    //获取ZKnode数据
    @Test
    public void getData() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zkClient.getData("/idea", false, null);
        System.out.println(new String(data, "utf-8"));
    }

    //删除节点数据
    @Test
    public void deleteNode() throws KeeperException, InterruptedException {

        //参数二表示要删除的版本，-1表示所有版本
        zkClient.delete("/idea", -1);
    }

    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkClient.setData("/idea", "hello wrold!".getBytes(), -1);
    }
}
