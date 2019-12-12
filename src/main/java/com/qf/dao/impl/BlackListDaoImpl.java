package com.qf.dao.impl;

import com.qf.dao.IBlackListDao;
import com.qf.entity.BlackList;
import com.qf.utils.DBCPUtil;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：游戏玩家黑名单操作dao层接口实现类<br/>
 * Copyright (c) ，2019 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2019年12月07日
 *
 * @author 徐文波
 * @version : 1.0
 */
public class BlackListDaoImpl implements IBlackListDao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getConnectionPool());

    @Override
    public void saveOrUpdate(BlackList entity) {
        try {
            qr.update("insert into tb_realtime_gamelog values(?,?,?) on duplicate key update avgTime=?,cnt=cnt+1",
                    entity.getUserName(),
                    entity.getAvgTime(),
                    entity.getCnt(),
                    entity.getAvgTime()
            );
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("操作开了外挂的玩家信息失败！异常信息是：" + e.getMessage());
        }
    }

    /**
     * 批量操作
     * @param beans
     */
    @Override
    public void batachDealWith(List<BlackList> beans) {

        try {
            String sql="insert into tb_realtime_gamelog values(?,?,?) on duplicate key update avgTime=?,cnt=cnt+1";

            Object[][] params = new Object[beans.size()][];

            for(int i=0;i<params.length;i++){
                BlackList bean = beans.get(i);
                params[i] = new Object[]{bean.getUserName(),bean.getAvgTime(),bean.getCnt(),bean.getAvgTime()};
            }

            //将sql送往db server去批量执行
            qr.batch(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量操作失败！异常信息是：" + e.getMessage());
        }

    }
}
