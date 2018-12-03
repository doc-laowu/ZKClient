package com.neusoft.ZKClientTest;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.util.ArrayList;
import java.util.List;


/**
 * zookeeper的事务操作
 */

public class zkTransaction {

    private ZooKeeper zk;
    private List<Op> ops = new ArrayList<Op>();

    protected zkTransaction(ZooKeeper zk) {
        this.zk = zk;
    }

    public zkTransaction create(final String path, byte data[], List<ACL> acl,
                              CreateMode createMode) {
        ops.add(Op.create(path, data, acl, createMode.toFlag()));
        return this;
    }

    public zkTransaction delete(final String path, int version) {
        ops.add(Op.delete(path, version));
        return this;
    }

    public zkTransaction check(String path, int version) {
        ops.add(Op.check(path, version));
        return this;
    }

    public zkTransaction setData(final String path, byte data[], int version) {
        ops.add(Op.setData(path, data, version));
        return this;
    }

    public List<OpResult> commit() throws InterruptedException, KeeperException {
        return zk.multi(ops);
    }

}
