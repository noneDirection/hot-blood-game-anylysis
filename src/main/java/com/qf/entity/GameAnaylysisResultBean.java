package com.qf.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: hotbloodgameanylysis
 * @description: 游戏项目离线分析结果bean
 * @author: youzhao
 * @create: 2019-12-05 16:48
 **/
@Data
@NoArgsConstructor
public class GameAnaylysisResultBean {
    /**
     * 基准日
     */
    private String date;
    /**
     * 新增用户
     */
    private long newAddUserCnt;
    /**
     * 活跃用户
     */
    private long activeUserCnt;
    /**
     * 次日留存率
     */
    private String nextDayRate;
    /**
     * 2日留存率
     */
    private String twoDayRate;
    /**
     * 3日留存率
     */
    private String threeDayRate;
    /**
     * 4日留存率
     */
    private String fourDayRate;
    /**
     * 5日留存率
     */
    private String fiveDayRate;
    /**
     * 6日留存率
     */
    private String sixDayRate;
    /**
     * 7日留存率
     */
    private String sevenDayRate;


    public GameAnaylysisResultBean(String date, long newAddUserCnt, long activeUserCnt, String nextDayRate, String twoDayRate, String threeDayRate, String fourDayRate, String fiveDayRate, String sixDayRate, String sevenDayRate) {
        this.date = date;
        this.newAddUserCnt = newAddUserCnt;
        this.activeUserCnt = activeUserCnt;
        this.nextDayRate = nextDayRate;
        this.twoDayRate = twoDayRate;
        this.threeDayRate = threeDayRate;
        this.fourDayRate = fourDayRate;
        this.fiveDayRate = fiveDayRate;
        this.sixDayRate = sixDayRate;
        this.sevenDayRate = sevenDayRate;
    }
}
