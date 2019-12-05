package com.qf.common;

/**
 * @program: hotbloodgameanylysis
 * @description: 共通的数据
 * @author: youzhao
 * @create: 2019-12-05 11:25
 **/
public interface CommonData {
    /**
     * 连接redis的共通的端口号
     */
    String REDIS_HOST_PORT = "redis.host.port";

    /**
     * 最大空闲连接数
     */
    String MAX_IDLE = "maxIdle";

    /**
     * 最大连接数
     */
    String MAX_TOTAL = "maxTotal";

    /**
     * 创建连接超时时间
     */
    String MAX_WAIT_MILLIS = "maxWaitMillis";

    /**
     * 获取连接测试是否可用
     */
    String TEST_ON_BORROW = "testOnBorrow";

    /**
     * maxAttempts
     */
    String MAX_ATTEMPTS = "maxAttempts";

    /**
     * 密码
     */
    String PASSWORD = "password";


    //_________________________________________

    //与TimeUtils相关的常量
    String TIME_PATTERN = "time.pattern";
    String TIME_PATTERN2 = "time.pattern2";
    String TIME_PATTERN3 = "time.pattern3";


    //_________________________________________
    //事件类型
    String REGISTER = "register";
    String LOGIN = "login";
    String LOGOUT = "logout";
    String UPGRADE = "upgrade";


    //_________________________________________
    String SPLIT_FLG="#";
    String SPLIT_FLG2=":";

    //_________________________________________
    String DB_CONN_PROPERTIES="db/dbcp.properties";
    String DB_CONN_EXCEPTION_INFO="数据库连接失败！";
}
