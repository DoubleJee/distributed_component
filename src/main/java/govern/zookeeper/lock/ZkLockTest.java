package govern.zookeeper.lock;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZkLockTest {

    private static int piao = 100;

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        ExecutorService executorService = Executors.newFixedThreadPool(15);
        for (int i = 1; i <= 20; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    ZkLock zkLock = null;
                    try {
                        zkLock = new ZkLock("buyPiao");
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    }
                    while (piao > 0) {
                        try {
                            zkLock.onLock();
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        if (piao > 0) {
                            System.out.println("我抢到了第" + piao + "张票");
                            piao--;
                        }
                        try {
                            zkLock.unLock();
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }
            });
        }
//        ZkLock zkLock = new ZkLock("buyPiao");
//        zkLock.onLock();
//        zkLock.unLock();
    }
}
