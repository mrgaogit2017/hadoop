package com.jhh.zk;

import org.I0Itec.zkclient.*;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 客户端监听zk节点和数据变化
 *
 * @author gaozhaolu
 * @date 2019/6/26 19:38
 */
public class ZookeeperLinster {
    private final static String zk_url = "172.16.11.201:2181,172.16.11.202:2182,172.16.11.203:2183,172.16.11.204:2183";
    private final static int time_out = 5000;
    private static final String PARENT_PATH = "/testWatch";
    private static final String CHILDREN_PATH = "/testWatch/children";

    /**
     * 基础操作:添加,访问, 删除节点
     */
    public static void baseZk() {
        ZkClient zkc = new ZkClient(new ZkConnection(zk_url), time_out);

        //订阅节点连接及状态的变化情况
        zkc.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) {
                // 连接状态判断
                if (keeperState == Watcher.Event.KeeperState.SyncConnected) {
                    //重新启动zk后，监听触发
                    System.out.println("连接成功");
                } else if (keeperState == Watcher.Event.KeeperState.Disconnected) {
                    //当我在服务端将zk服务stop时，监听触发
                    System.out.println("连接断开");
                } else {
                    System.out.println("其他状态" + keeperState);
                }
            }

            @Override
            public void handleNewSession() {
                System.out.println("handleNewSession");
            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) {
                System.out.println("handleSessionEstablishmentError");
            }
        });
        // 创建节点
        zkc.createPersistent(PARENT_PATH, "1234");
        // 读取节点数据，并且输出
        String data = zkc.readData(PARENT_PATH);
        System.out.println(data);
        // 更新节点数据
        zkc.writeData(PARENT_PATH, "4567");
        String data2 = zkc.readData(PARENT_PATH);
        System.out.println(data2);

        // 创建子节点
        zkc.createPersistent(CHILDREN_PATH, "children");
        String data3 = zkc.readData(CHILDREN_PATH);
        System.out.println(data3);
        // 判断节点是否存在
        boolean exists = zkc.exists(PARENT_PATH);
        System.out.println(exists);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 递归的删除节点
        zkc.deleteRecursive(PARENT_PATH);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zkc.close();
    }

    /**
     * 监控一个节点数据变化
     */
    public static void watcherDataChanges() {
        ZkClient zkc = new ZkClient(new ZkConnection(zk_url), time_out);
        // 注册节点改变的信息变化
        zkc.subscribeDataChanges(PARENT_PATH, new IZkDataListener() {

            @Override
            public void handleDataDeleted(String path) throws Exception {
                System.out.println("subscribeDataChanges    handleDataDeleted");
                System.out.println("删除的节点为:" + path);
            }

            @Override
            public void handleDataChange(String path, Object data) throws Exception {
                System.out.println("subscribeDataChanges    handleDataChange");
                System.out.println("变更的节点为:" + path + ", 变更内容为:" + data);
            }
        });

        // 创建节点
        zkc.createPersistent(PARENT_PATH, "1234");
        // 读取节点数据，并且输出
        String data = zkc.readData(PARENT_PATH);
        System.out.println(data);
        // 更新节点数据
        zkc.writeData(PARENT_PATH, "4567");
        String data2 = zkc.readData(PARENT_PATH);
        System.out.println(data2);

        // 删除节点
        zkc.deleteRecursive(PARENT_PATH);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zkc.close();
    }

    /**
     * 监控子节点变化
     */
    public static void watcherChildChanges() {
        ZkClient zkc = new ZkClient(new ZkConnection(zk_url), time_out);
        // 统一序列化, 主要为了读取非本客户端注册的节点信息
        zkc.setZkSerializer(new MyZkSerializer());
        // 注册节点孩子信息变化
        /*zkc.subscribeChildChanges(PARENT_PATH, new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("subscribeChildChanges handleChildChange");
                System.out.println("parentPath: " + parentPath);
                System.out.println("currentChilds: " + currentChilds);
            }
        });*/

        // 创建节点
        if (!zkc.exists(PARENT_PATH)) {
            zkc.createPersistent(PARENT_PATH, "天气好热1234");
        }

        // 读取节点数据，并且输出
        String data = zkc.readData(PARENT_PATH);
        System.out.println(data);

        // 创建子节点
        zkc.createPersistent(CHILDREN_PATH, "children");
        // 创建第二个子节点
        String CHILDREN_PATH_2 = "/testWatch/children2";
        zkc.createPersistent(CHILDREN_PATH_2, "this is children2");

        // 修改子节点的数据(不会触发subscribeChildChanges)
        zkc.writeData(CHILDREN_PATH, "hahaha");

        // 遍历子节点
        String ha_parent_path = "/hadoop-ha/bi";
        boolean b = zkc.exists(ha_parent_path);
        if (!b) {
            return;
        }

        List<String> childs = zkc.getChildren(ha_parent_path);
        for (String childPath : childs) {
            childPath = ha_parent_path+"/"+childPath;
            String childata = zkc.readData(childPath);
            System.out.println("path:"+childPath+", value:"+childata);
        }
        if (childs != null) {
            return;
        }

        // 删除子节点
        zkc.deleteRecursive(CHILDREN_PATH);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 删除节点
        zkc.deleteRecursive(PARENT_PATH);
        zkc.close();
    }

    public static class MyZkSerializer implements ZkSerializer
    {
        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError
        {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] serialize(Object obj) throws ZkMarshallingError
        {
            return String.valueOf(obj).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static void main(String[] args) {
        //baseZk();
        //watcherDataChanges();
        watcherChildChanges();
    }
}
