package govern.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperTest implements Watcher {

    private ZooKeeper zooKeeper;

    //用来表示zookeeper连接成功
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    //观察者事件通知
    @Override
    public void process(WatchedEvent event) {
        System.out.println("触发了事件：" + event.getState() + "、" + event.getPath() + "、" + event.getType());
        if(event.getState().equals(Event.KeeperState.SyncConnected)){
            countDownLatch.countDown();
        }
    }

    public ZookeeperTest(String host) throws IOException, InterruptedException {
        //zk 连接地址，超时事件，与观察者处理实例
        zooKeeper = new ZooKeeper(host,10000,this);
        countDownLatch.await();
        System.out.println("zookeeper服务器连接成功");
    }

    //创建zk 临时有序节点
    public String createNode(String path,String data) throws KeeperException, InterruptedException {
        String s = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        return s;
    }

    //获取zk 节点下的子节点
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(path, this);
        return children;
    }

    //获取zk 节点数据
    public String getNodeData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, this, null);
        return new String(data);
    }

    //设置zk 节点值
    public void setNodeData(String path,String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    //删除zk 节点
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        zooKeeper.delete(path,-1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperTest zookeeperTest = new ZookeeperTest("129.204.89.67:2181");
        String testNode = "/gzz_test";
        List<String> children = zookeeperTest.getChildren("/");
        System.out.println("该节点下子集节点" + children.toString());
        zookeeperTest.createNode(testNode, "lock");

        //修改节点值
        String nodeData = zookeeperTest.getNodeData(testNode);
        System.out.println(testNode + "的data：" + nodeData);
        zookeeperTest.setNodeData(testNode,"newLock");
        nodeData = zookeeperTest.getNodeData(testNode);
        System.out.println(testNode + "的data：" + nodeData);

        zookeeperTest.setNodeData(testNode,"AgainLock");
        nodeData = zookeeperTest.getNodeData(testNode);
        System.out.println(testNode + "的data：" + nodeData);

        zookeeperTest.deleteNode(testNode);
        System.out.println(testNode + "节点删除成功！");
    }

}
