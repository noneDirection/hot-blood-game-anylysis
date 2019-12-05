package com.qf.utils;

import com.qf.common.CommonData;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @program: hotbloodgameanylysis
 * @description:DBCP连接池工具类
 * @author: youzhao
 * @create: 2019-12-05 12:46
 **/
public class DBCPUtil {
    //连接池
    private static DataSource pool;

    //静态代码段初始化连接池
    static {
        Properties properties = new Properties();
        try {
            properties.load(DBCPUtil.class.getClassLoader().getResourceAsStream(CommonData.DB_CONN_PROPERTIES));
            pool = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * @Description 获得连接的实例
     * @Param []
     * @return java.sql.Connection
     */
    public static Connection getConnection(){
        try {
            return pool.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(CommonData.DB_CONN_EXCEPTION_INFO);
        }
    }
    /**
     * @Description 返回连接的实例
     * @Param []
     * @return javax.sql.DataSource
     */
    public static DataSource getConnectionPool() {
        return pool;
    }
}
