package com.qf.common;
import com.qf.utils.CommonUtil;
/**
 * @program: hotbloodgameanylysis
 * @description:
 * * 事件类型枚举
 * 0 管理员登陆
 * 1 注册或首次登陆
 * 2 上线，登录
 * 3 下线
 * 4 升级
 * 5 预留
 * 6 装备回收元宝
 * 7 元宝兑换RMB
 * 8 PK
 * 9 成长任务
 * 10 领取奖励
 * 11 神力护身
 * 12 购买物品
 * @author: youzhao
 * @create: 2019-12-05 11:55
 **/
public enum EventType {
    REGISTER(CommonUtil.getPropertiesValueByKey(CommonData.REGISTER)),
    LOGIN(CommonUtil.getPropertiesValueByKey(CommonData.LOGIN)),
    LOGOUT(CommonUtil.getPropertiesValueByKey(CommonData.LOGOUT)),
    UPGRADE(CommonUtil.getPropertiesValueByKey(CommonData.UPGRADE));

    /**
     * 事件类型
     */
    private String eventType;


    public String getEventType() {
        return eventType;
    }

    EventType(String type) {
        this.eventType = type;
    }
}
