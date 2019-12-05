package com.qf.dao;

import com.qf.entity.GameAnaylysisResultBean;

/**
 * @program: hotbloodgameanylysis
 * @description: 游戏项目离线分析结果dao层接口
 * @author: youzhao
 * @create: 2019-12-05 16:32
 **/
public interface IGameAnaylysisResult {
    /**
     * 保存统计的结果到db中
     *
     * @param bean
     */
    void save(GameAnaylysisResultBean bean);
}
