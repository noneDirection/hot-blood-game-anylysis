package com.qf.dao.impl;

import com.qf.dao.IGameAnaylysisResult;
import com.qf.entity.GameAnaylysisResultBean;
import com.qf.utils.DBCPUtil;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;

/**
 * @program: hotbloodgameanylysis
 * @description: 游戏项目离线分析结果dao层接口实现类
 * @author: youzhao
 * @create: 2019-12-05 16:50
 **/
public class GameAnaylysisResultImpl implements IGameAnaylysisResult{

    private QueryRunner qr = new QueryRunner(DBCPUtil.getConnectionPool());
    @Override
    public void save(GameAnaylysisResultBean bean) {
        try {
            qr.update("insert into tb_game_anaylysis_result values(?,?,?,?,?,?,?,?,?,?) on duplicate key update " +
                            "newAddUserCnt=?," +
                            "activeUserCnt=?," +
                            "nextDayRate=?," +
                            "twoDayRate=?," +
                            "threeDayRate=?," +
                            "fourDayRate=?," +
                            "fiveDayRate=?," +
                            "sixDayRate=?," +
                            "sevenDayRate=?",
                    bean.getDate(),
                    bean.getNewAddUserCnt(),
                    bean.getActiveUserCnt(),
                    bean.getNextDayRate(),
                    bean.getTwoDayRate(),
                    bean.getThreeDayRate(),
                    bean.getFourDayRate(),
                    bean.getFiveDayRate(),
                    bean.getSixDayRate(),
                    bean.getSevenDayRate(),

                    bean.getNewAddUserCnt(),
                    bean.getActiveUserCnt(),
                    bean.getNextDayRate(),
                    bean.getTwoDayRate(),
                    bean.getThreeDayRate(),
                    bean.getFourDayRate(),
                    bean.getFiveDayRate(),
                    bean.getSixDayRate(),
                    bean.getSevenDayRate()
            );
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("保存到db中失败了！异常信息是：" + e.getMessage());
        }
    }
}
