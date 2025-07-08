package com.wy.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * 作者: wangyang <br/>
 * 创建时间: 2025/6/13 <br/>
 * 描述: <br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;MysqlUtil
 * 权限库的连接池，不做静态类，因为需要获取hive的配置对象数据，然后才初始化
 */
public class MysqlUtil {
    private HikariDataSource dataSource ;
    private HikariConfig config;
    private ArrayList<Connection> conns;
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
        config = new HikariConfig();

        config.setJdbcUrl(url);
        config.setDriverClassName(driver);
        config.setConnectionTimeout(timeout);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(hp_maxsize);
        config.setMinimumIdle(hp_minidle);
        config.setIdleTimeout(hp_id_timeout);
        config.setMaxLifetime(hp_lefttime);

        dataSource = new HikariDataSource(config);
        conns = new ArrayList<Connection>(hp_maxsize);
    }

    /**
     * 获取连接的方法
     */
    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        conns.add(connection);
        return connection;
    }

    /**
     * 关闭单个连接的方法，但是不关连接池
     */
    public void closeConnection(Connection connection)  {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("鉴权数据连接池资源异常 "+e);
        }
    }

    /**
     * 关闭所有连接的方法，但是不关连接池
     */
    public void closeAllConnection() {
        for (Connection connection : conns) {
            closeConnection(connection);
        }
        conns.clear();
    }

}
