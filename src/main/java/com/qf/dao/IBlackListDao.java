package com.qf.dao;

import com.qf.entity.BlackList;

import java.util.List;

/**
 * Description：游戏玩家黑名单操作dao层接口<br/>
 * Copyright (c) ，2019 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2019年12月07日
 *
 * @author 徐文波
 * @version : 1.0
 */
public interface IBlackListDao {
    /**
     * 保存或是更新
     *
     * @param entity
     */
    void saveOrUpdate(BlackList entity);


    /**
     * 批量操作
     *
     * @param beans
     */
    void batachDealWith(List<BlackList> beans);
}
