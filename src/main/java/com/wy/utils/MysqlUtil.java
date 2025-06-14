package com.wy.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作者: wangyang <br/>
 * 创建时间: 2025/6/13 <br/>
 * 描述: <br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;MysqlUtil
 * 权限库的连接池，和查询鉴权的 SqlFieldAuthCheckUtil 一样不做静态类
 * 但是链接池用到的配置是静态资源，毕竟执行sql可能同一个会话有多个，但是会话启动之后当前会话自身只有一个
 * 也就是：一个会话下 ， 会有多个sql ， 每个sql鉴权和连接池对象都是sql自己单独的 ， 但连接池用的配置，是会话启动时加载好的
 */
public class MysqlUtil {
    private HikariDataSource dataSource = null;

    private static String url;
    private static String dirver;
    private static Long timeout;
    private static String username;
    private static String password;
    private static int hp_maxsize;
    private static int hp_minidle;
    private static long hp_id_timeout;
    private static long hp_lefttime;

    /**
     * 初始化配置的方法
     * @param url
     * @param username
     * @param password
     * @param hp_maxsize
     * @param hp_minidle
     * @param hp_id_timeout
     * @param hp_lefttime
     */
    public static void init(String url,String driver,Long timeout,String username,String password,int hp_maxsize,int hp_minidle,long hp_id_timeout,long hp_lefttime){
        MysqlUtil.url = url;
        MysqlUtil.dirver = driver;
        MysqlUtil.timeout = timeout;
        MysqlUtil.username = username;
        MysqlUtil.password = password;
        MysqlUtil.hp_maxsize = hp_maxsize;
        MysqlUtil.hp_minidle = hp_minidle;
        MysqlUtil.hp_id_timeout = hp_id_timeout;
        MysqlUtil.hp_lefttime = hp_lefttime;
    }

    /**
     * 构造器中对数据库连接池初始化
     */
    public MysqlUtil() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(MysqlUtil.url);
        config.setDriverClassName(MysqlUtil.dirver);
        config.setConnectionTimeout(MysqlUtil.timeout);
        config.setUsername(MysqlUtil.username);
        config.setPassword(MysqlUtil.password);
        config.setMaximumPoolSize(MysqlUtil.hp_maxsize);
        config.setMinimumIdle(MysqlUtil.hp_minidle);
        config.setIdleTimeout(MysqlUtil.hp_id_timeout);
        config.setMaxLifetime(MysqlUtil.hp_lefttime);
        dataSource = new HikariDataSource(config);
    }

    /**
     * 代码中动态指定连接池大小的构造器
     * @param hp_maxsize
     * @param hp_minidle
     */
    public MysqlUtil(int hp_maxsize,int hp_minidle) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(MysqlUtil.url);
        config.setDriverClassName(MysqlUtil.dirver);
        config.setConnectionTimeout(MysqlUtil.timeout);
        config.setUsername(MysqlUtil.username);
        config.setPassword(MysqlUtil.password);
        config.setMaximumPoolSize(hp_maxsize);
        config.setMinimumIdle(hp_minidle);
        config.setIdleTimeout(MysqlUtil.hp_id_timeout);
        config.setMaxLifetime(MysqlUtil.hp_lefttime);
        dataSource = new HikariDataSource(config);
    }

    /**
     * 获取连接的方法
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 关闭连接的方法，但是不关连接池
     */
    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭连接池
     */
    public void closePool(){
        if ( !dataSource.isClosed() ){
            dataSource.close();
        }
    }

    /**
     * 让鉴权时的线程池知道有多少个sql连接线程可用
     * @return
     */
    public static int getHp_maxsize() {
        return hp_maxsize;
    }
}
