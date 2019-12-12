package com.qf.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：游戏玩家黑名单实体类<br/>
 * Copyright (c) ，2019 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2019年12月07日
 *
 * @author 徐文波
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class BlackList {
    /**
     * 玩家名
     */
    private String userName;

    /**
     * 玩家每次吃疗伤药的平均时间
     */
    private double avgTime;

    /**
     * 开了外挂的玩家被警告的次数 （临界值3，>3,立马注销账户；若：<=3, 连续一周没有开过外挂，将信息从表中清除）
     */
    private int cnt;

    public BlackList(String userName, double avgTime, int cnt) {
        this.userName = userName;
        this.avgTime = avgTime;
        this.cnt = cnt;
    }
}
