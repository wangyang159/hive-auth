package com.wy.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

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
    private final HikariDataSource dataSource ;

    private final String url;
    private final String driver;
    private final Long timeout;
    private final String username;
    private final String password;
    private final int hp_maxsize;
    private final int hp_minidle;
    private final long hp_id_timeout;
    private final long hp_lefttime;

    /**
     * 全参数构造，使用时的数值类型检查不写在这里，是因为参数性质的异常在服务或者模块被调起时爆发，比开始跑服务new这个对象才爆出来合理
     * @param url
     * @param driver
     * @param timeout
     * @param username
     * @param password
     * @param hp_maxsize
     * @param hp_minidle
     * @param hp_id_timeout
     * @param hp_lefttime
     */
    public MysqlUtil(String url, String driver, Long timeout, String username, String password, int hp_maxsize, int hp_minidle, long hp_id_timeout, long hp_lefttime) {
        HikariConfig config = new HikariConfig();

        this.url = url;
        config.setJdbcUrl(url);

        this.driver = driver;
        config.setDriverClassName(driver);

        this.timeout = timeout;
        config.setConnectionTimeout(timeout);

        this.username=username;
        config.setUsername(username);

        this.password = password;
        config.setPassword(password);

        this.hp_maxsize = hp_maxsize;
        config.setMaximumPoolSize(hp_maxsize);

        this.hp_minidle = hp_minidle;
        config.setMinimumIdle(hp_minidle);

        this.hp_id_timeout = hp_id_timeout;
        config.setIdleTimeout(hp_id_timeout);

        this.hp_lefttime = hp_lefttime;
        config.setMaxLifetime(hp_lefttime);

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

}
