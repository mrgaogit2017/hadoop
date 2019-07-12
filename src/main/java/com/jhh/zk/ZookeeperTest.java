package com.jhh.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 *
 * ZooKeeper
 * 这个类是zk原生类,并不好用
 *
 * 建议参考学习ZookeeperLinster中的第三方客户端类ZkClient
 *
 *
 * @author gaozhaolu
 * @date 2019/6/26 14:53
 */
public class ZookeeperTest {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private final static String zk_url = "172.16.11.201:2181,172.16.11.202:2182,172.16.11.203:2183,172.16.11.204:2183";

    private final static int time_out = 5000;

    public static void main(String[] arges) throws IOException, InterruptedException {
        //初始化zk, 参数1 zk地址, 参数2 超时时间, 参数3 监听器
        ZooKeeper zooKeeper = new ZooKeeper(zk_url, time_out, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                Watcher.Event.KeeperState state = watchedEvent.getState();
                Event.EventType type = watchedEvent.getType();
                if (Event.KeeperState.SyncConnected == state) {
                    if (Event.EventType.None == type) {
                        System.out.println("成功连接zk服务器！");
                        //调用此方法测计数减一
                        countDownLatch.countDown();
                    }
                }
            }
        });
        //阻碍当前线程进行,等待连接zk的线程执行完毕
        countDownLatch.await();

        try {
            //创建持久化节点
            //zooKeeper.create("/gang", "你好".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // 注册服务
            zooKeeper.create("/pay", "172.16.11.49:8088".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            //获取节点数据
            byte[] data = zooKeeper.getData("/hadoop-ha/bi", false, null);
            System.out.println(new String(data));
            //修改节点数据
            zooKeeper.setData("/gang", "吕金刚".getBytes(), 0);
            //删除节点数据
            zooKeeper.delete("/gang", -1);
            //创建临时节点 异步创建
            zooKeeper.create("/jingang", "临时节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
                @Override
                public void processResult(int i, String s, Object o, String s1) {
                    System.out.println(o);
                    System.out.println(i);
                    System.out.println(s1);
                    System.out.println(s);
                }
            }, "a");
            //获取临时节点数据
            byte[] jingangs = zooKeeper.getData("/jingang", false, null);
            System.out.println(new String(jingangs));
            //验证节点是否存在
            Stat exists = zooKeeper.exists("/jingang", false);
            System.out.println(exists);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zooKeeper.close();


    }
}
