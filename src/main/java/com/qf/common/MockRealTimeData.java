package com.qf.common;

import java.io.*;

/**
 * @program: hotbloodgameanylysis
 * @description: 随机生成测试数据
 * @author: youzhao
 * @create: 2019-12-06 14:51
 **/
public class MockRealTimeData extends Thread{
    @Override
    public void run(){
        StringBuffer buffer = new StringBuffer();
        File dir = new File("dir/data");
        File[] dirs = dir.listFiles();
        for(File tempDir:dirs){
            File[] logFiles = tempDir.listFiles();
            for(File file:logFiles){
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GB2312"));
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                }

            }
        }
    }
}
