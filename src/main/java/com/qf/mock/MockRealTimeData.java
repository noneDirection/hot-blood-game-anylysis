package com.qf.mock;

import com.qf.utils.CommonUtil;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Description：模拟实时产生的数据，写入到游戏日志文件中</>
 * 造数的程序(开启进程) →向日志文件中实时写入数据 （对应的业务场景：游戏玩家玩游戏时，实时向日志文件中写入数据）
 * <br/>
 * Copyright (c) ，2019 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2019年12月06日
 *
 * @author 徐文波
 * @version : 1.0
 */
public class MockRealTimeData extends Thread {


    @Override
    public void run() {
        List<String> container = readGameGlog2Buffer();
        String targetFile = "a_data/output/gameRealTimeLog.txt";

        BufferedWriter bw = null;
        try {
            // RandomAccessFile raf = new RandomAccessFile("");

            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile), "GB2312"));

            int index = 0;

            while (true) {
                //计数
              String log=  container.get(index++);

                //步骤：
                //①读取指定集合中的日志信息
                //②写入指定的日志文件
                bw.write(log);
                bw.newLine();
                bw.flush();

                //③sleep
                CommonUtil.mySleep(1);

                //若是计数器中的值与集合长度相同，从新读取文件中的数据
                if (index == container.size()) {
                    index=0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    /**
     * 将游戏日志文件中的数据读取到StringBuffer中
     */
    private static List<String> readGameGlog2Buffer() {
        List<String> buffer = new LinkedList<>();

        File dir = new File("a_data/input");

        File[] dirs = dir.listFiles();
        for (File tmpDir : dirs) {
            File[] logFiles = tmpDir.listFiles();
            for (File file : logFiles) {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GB2312"));

                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        buffer.add(line);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        return buffer;
    }

    public static void main(String[] args) {
        new MockRealTimeData().start();
    }

}
