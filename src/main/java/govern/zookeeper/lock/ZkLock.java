package govern.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

public class ZkLock implements Watcher {

    private String host = "129.204.89.67:2181";

    private ZooKeeper zooKeeper;

    //锁的根节点
    private String rootLockName = "/locks";

    //业务锁
    private String lockName;

    private String myLock;

    //用来表示zookeeper连接成功
    private CountDownLatch connectDownLatch = new CountDownLatch(1);

    private CountDownLatch lockDownLatch;

    @Override
    public void process(WatchedEvent event) {
//        System.out.println("触发了事件：" + event.getState() + "、" + event.getPath() + "、" + event.getType());
        if (event.getState().equals(Event.KeeperState.SyncConnected)) {
            connectDownLatch.countDown();
        }
        //上一个锁节点被删除的时候释放等待
        if (event.getType().equals(Event.EventType.NodeDeleted)) {
            if (lockDownLatch != null) {
                lockDownLatch.countDown();
            }
        }
    }

    public ZkLock(String lockName) throws IOException, InterruptedException, KeeperException {
        this.lockName = rootLockName + "/" + lockName;
        zooKeeper = new ZooKeeper(host, 10000, this);
        connectDownLatch.await();
        Stat rootNode = zooKeeper.exists(rootLockName, false);
        if (rootNode == null) {
            zooKeeper.create(rootLockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        Stat lockNode = zooKeeper.exists(this.lockName, false);
        if (lockNode == null) {
            zooKeeper.create(this.lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    //上锁
    public void onLock() throws KeeperException, InterruptedException {
        String lockTemplate = lockName + "/lock";
        myLock = zooKeeper.create(lockTemplate, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        List<String> children = zooKeeper.getChildren(lockName, false);
        TreeSet<String> sortedNodes = new TreeSet<>();
        for(String node : children) {
            sortedNodes.add(lockName + "/" +node);
        }
        String first = sortedNodes.first();
        String prev = sortedNodes.lower(myLock);
        if(myLock.equals(first)){
            //获取锁
            System.out.println(Thread.currentThread().getName() + "获取了锁：" + myLock);
            return;
        }

        Stat prevNode = zooKeeper.exists(prev, this);
        if(prevNode != null){
            System.out.println(Thread.currentThread().getName() + "等待上一个锁释放：" + prev);
            lockDownLatch = new CountDownLatch(1);
            lockDownLatch.await();
            System.out.println(Thread.currentThread().getName() + "获取了锁：" + myLock);
            lockDownLatch = null;
        }

    }

    //释放锁
    public void unLock() throws KeeperException, InterruptedException {
        if(this.myLock == null){
            throw new InterruptedException("1");
        }
        zooKeeper.delete(this.myLock,-1);
        System.out.println(Thread.currentThread().getName() + "释放了锁：" + this.myLock);
        this.myLock = null;
    }


}
