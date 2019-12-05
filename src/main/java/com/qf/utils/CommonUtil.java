package com.qf.utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
/**
 * @program: hotbloodgameanylysis
 * @description: 共通的工具类
 * @author: youzhao
 * @create: 2019-12-05 11:58
 **/
public class CommonUtil {
    /**
     * @Description 加载resources资源目录下共通的资源文件application.properties
     * @Param []
     * @return com.typesafe.config.Config
     */
    public static Config loadApplicationPropertiesFile(){
        return ConfigFactory.load();
    }
    /**
     * @Description 根据资源文件中的key获得对应的value
     * @Param [key]
     * @return java.lang.String
     */
    public static String getPropertiesValueByKey(String key){
        return loadApplicationPropertiesFile().getString(key);
    }
    /**
     * @Description 自定义睡眠时间
     * @Param [second]
     * @return void
     */
    public static void mySleep(int second){
        try {
            Thread.sleep(second);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}
