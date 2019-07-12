package com.jhh.hash;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * 一致性hash的java实现
 *
 * @author gaozhaolu
 * @date 2019/7/12 10:22
 */
public class ConsistentHashWithVirtualNode {
    /**
     * 待添加入Hash环的服务器列表
     */
    private static String[] SERVERS = {"192.168.1.2:6379", "192.168.1.3:6379", "192.168.1.4:6379", "192.168.1.5:6379"};

    /**
     * key表示服务器的hash值，value表示虚拟节点的名称
     */
    private static SortedMap<Integer, String> HASH_CIRCLE = new TreeMap<>();

    /**
     * 用于结果统计
     */
    private static Map<String, Integer> result = new HashMap<>();

    /**
     * 每个真实节点对应虚拟节点数
     */
    private static Integer VIRTUAL_NODES_NUM = 100;

    /**
     * 使用FNV1_32_HASH算法计算服务器的Hash值
     */
    private static int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash ^ str.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        // 如果值为负数则取其绝对值
        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }

    static {
        for (int i = 0; i < SERVERS.length; i++) {
            for (Integer j = 1; j <= VIRTUAL_NODES_NUM; j++) {
                setServer(SERVERS[i] + "vn" + j);
            }
        }
    }

    private static void setServer(String ip) {
        setServer(ip, null);
    }


    private static void setServer(String ip, Integer hash) {
        hash = getHash(hash == null ? ip : hash.toString());
        if (StringUtils.isBlank(HASH_CIRCLE.get(hash))) {
            HASH_CIRCLE.put(hash, ip);
            //System.out.println("[" + ip + "]加入sortedMap中, 其Hash值为" + hash);
        } else {
            //解决hash碰撞
            setServer(ip, hash);
        }
    }


    public static void main(String[] args) {
        long totalRequests = 50000;
        for (int i = 0; i < totalRequests; i++) {
            long nodes = RandomUtils.nextLong();
            String server = getServer(nodes);
            String realServer = server.split("vn")[0];
            //System.out.println("[" + nodes + "]的hash值为" + getHash("" + nodes) + ", 被路由到虚拟结点[" + server + "], 真实结点[" + realServer + "]");
            result.put(realServer, (result.get(realServer) == null ? 0 : result.get(realServer)) + 1);
        }
        result.forEach((k, v) -> System.out.println("结点[" + k + "]上有" + v + "个负载"));
    }

    public static String getServer(Object node) {
        String ip = HASH_CIRCLE.get(HASH_CIRCLE.firstKey());
        // 得到带路由的结点的Hash值
        int hash = getHash(node.toString());
        // 得到大于该Hash值的所有Map
        SortedMap<Integer, String> subMap = HASH_CIRCLE.tailMap(hash);

        if (!subMap.isEmpty()) {
            // 第一个Key就是顺时针过去离node最近的那个结点
            ip = subMap.get(subMap.firstKey());
        }
        // 返回对应的服务器名称
        return ip;
    }
}
