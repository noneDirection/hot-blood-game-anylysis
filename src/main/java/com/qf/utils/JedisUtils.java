package com.qf.utils;

import com.qf.common.CommonData;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * @program: hotbloodgameanylysis
 * @description: Redis内存操作数据库
 * @author: youzhao
 * @create: 2019-12-05 13:09
 **/
public class JedisUtils {
    //准备容器 存储JedisCluster池的实例
    private static LinkedList<JedisCluster> pool;

    //对JedisCluster池实例
    static {
        pool = new LinkedList<>();
        for(int i=1; i<=10; i++){
            JedisCluster instance = createNewJedisCluster();
            pool.add(instance);
        }
    }
    /**
     * @Description 创建JedisCluster实例
     * @Param []
     * @return redis.clients.jedis.JedisCluster
     */
    private static JedisCluster createNewJedisCluster() {
        //Set<HostAndPort> nodes
        String hostAndPortInfos = CommonUtil.getPropertiesValueByKey(CommonData.REDIS_HOST_PORT);
        Set<HostAndPort> nodes = new LinkedHashSet<>();
        String[] arr = hostAndPortInfos.split(CommonData.SPLIT_FLG);
        for (int i = 0; i < arr.length; i++) {
            String[] hostAndPort = arr[i].split(CommonData.SPLIT_FLG2);
            String host = hostAndPort[0].trim();
            int port = Integer.parseInt(hostAndPort[1].trim());
            nodes.add(new HostAndPort(host, port));
        }


        //int timeout
        int timeout = Integer.parseInt(CommonUtil.getPropertiesValueByKey(CommonData.MAX_WAIT_MILLIS));

        // GenericObjectPoolConfig poolConfig
        GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(Integer.parseInt(CommonUtil.getPropertiesValueByKey(CommonData.MAX_IDLE)));
        poolConfig.setMaxTotal(Integer.parseInt(CommonUtil.getPropertiesValueByKey(CommonData.MAX_TOTAL)));
        poolConfig.setMaxWaitMillis(timeout);
        poolConfig.setTestOnBorrow(Boolean.valueOf(CommonUtil.getPropertiesValueByKey(CommonData.TEST_ON_BORROW)));


        //maxAttempts
        int maxAttempts = Integer.parseInt(CommonUtil.getPropertiesValueByKey(CommonData.MAX_ATTEMPTS));

        //password
        String password = CommonUtil.getPropertiesValueByKey(CommonData.PASSWORD);

        return new JedisCluster(nodes, timeout, timeout, maxAttempts, password, poolConfig);
    }
    /**
     * @Description 从JedisCluster池中获得JedisCluster的实例
     * @Param
     * @return
     */
    public static JedisCluster getJedisClusterInstanceFromPool() {
        while(pool.size() == 0){
            CommonUtil.mySleep(1);
        }
        return pool.pop();
    }
    /**
     * @Description 进行资源释放(伪释放，不是将JedisCluster实例close，而是重新置于池中供别的线程使用)
     * @Param [cluster]
     * @return void
     */
    public static void resourceRelease(JedisCluster cluster) {
        if (cluster != null) {
            pool.push(cluster);
        }
    }
}
