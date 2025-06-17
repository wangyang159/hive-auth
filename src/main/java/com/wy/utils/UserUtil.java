package com.wy.utils;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.shims.Utils;

import javax.security.auth.login.LoginException;
import java.io.IOException;

/**
 * 用户相关操作的工具类
 */
public class UserUtil {

    /**
     * 获取当前hive连接用户的身份
     * @return
     * @throws MetaException
     */
    public static String getUserName() throws MetaException {
        String userName = null;
        try {
            /*
             * 元数据的监听类在Pre中要鉴权所以获取user，而在元数据更改之后的MyMetaStoreEventListener类中不需要鉴权
             */
            userName = Utils.getUGI().getUserName();
            if (userName == null || userName.length() == 0 || userName.equals("null") || userName.equals("undefined") || userName.equals("")) {
                throw new MetaException("用户非法");
            }
        } catch (IOException | LoginException e) {
            throw new MetaException("用户非法");
        }
        return userName;
    }

}
