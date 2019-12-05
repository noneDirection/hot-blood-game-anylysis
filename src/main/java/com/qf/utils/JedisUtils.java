package com.qf.utils;

import com.qf.common.CommonData;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedList;

/**
 * @program: hotbloodgameanylysis
 * @description: Redis内存操作数据库
 * @author: youzhao
 * @create: 2019-12-05 13:09
 **/
public class JedisUtils {
    //准备容器 存储Jedis池的实例
    private static LinkedList<Jedis> pool;

    //对Jedis池实例
    static {
        pool = new LinkedList<>();
        for(int i=1; i<=10; i++){
            Jedis instance = createNewJedis();
            pool.add(instance);
        }
    }
    /**
     * @Description 创建Jedis实例
     * @Param []
     * @return redis.clients.jedis.Jedis
     */
    private static Jedis createNewJedis() {
        //Set<HostAndPort> nodes
        String []hostAndPortInfos = CommonUtil.getPropertiesValueByKey(CommonData.REDIS_HOST_PORT).split(":");
        String host = hostAndPortInfos[0];
        int port = Integer.parseInt(hostAndPortInfos[1]);
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


        return new Jedis(host, port, timeout, timeout);
    }
    /**
     * @Description 从Jedis池中获得Jedis的实例
     * @Param
     * @return
     */
    public static Jedis getJedisInstanceFromPool() {
        while(pool.size() == 0){
            CommonUtil.mySleep(1);
        }
        return pool.pop();
    }
    /**
     * @Description 进行资源释放(伪释放，不是将Jedis实例close，而是重新置于池中供别的线程使用)
     * @Param [cluster]
     * @return void
     */
    public static void resourceRelease(Jedis cluster) {
        if (cluster != null) {
            pool.push(cluster);
        }
    }
}
