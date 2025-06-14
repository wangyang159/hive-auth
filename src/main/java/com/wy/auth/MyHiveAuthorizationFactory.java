package com.wy.auth;

import com.wy.utils.MysqlUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.*;

import java.math.BigInteger;

public class MyHiveAuthorizationFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory hiveMetastoreClientFactory, HiveConf hiveConf, HiveAuthenticationProvider hiveAuthenticationProvider, HiveAuthzSessionContext hiveAuthzSessionContext) throws HiveAuthzPluginException {
        //初始化鉴权库的连接池
        String url = hiveConf.get("hive.auth.database.url");
        String username = hiveConf.get("hive.auth.database.username");
        String password = hiveConf.get("hive.auth.database.password");
        String hp_maxsize = hiveConf.get("hive.auth.database.hikari.pool.maxsize");
        String hp_minidle = hiveConf.get("hive.auth.database.hikari.pool.minidle");
        String hp_id_timeout = hiveConf.get("hive.auth.database.hikari.pool.idle.timeout");
        String hp_lefttime = hiveConf.get("hive.auth.database.hikari.pool.max.lifetime");

        //数据库连接池大小校验
        BigInteger hp_maxsize_bi = new BigInteger(hp_maxsize);
        if ( hp_maxsize_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_maxsize_bi.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0 ){
            throw new HiveAuthzPluginException("鉴权连接池大小超过预期Int值");
        }

        //数据库连接池空闲连接大小校验
        BigInteger hp_minidle_bi = new BigInteger(hp_minidle);
        if ( hp_minidle_bi.compareTo(BigInteger.valueOf(2)) < 0 || hp_minidle_bi.compareTo(BigInteger.valueOf( hp_maxsize_bi.intValue() / 2 )) > 0   ){
            throw new HiveAuthzPluginException("鉴权连接池空闲连接大小超过预期Int值");
        }

        //数据库连接池空闲超时校验
        BigInteger hp_id_timeout_bi = new BigInteger(hp_id_timeout);
        if ( hp_id_timeout_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_id_timeout_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0  ){
            throw new HiveAuthzPluginException("鉴权连接池空闲超时时间超过预期Long值");
        }

        //数据库连接池连接最大存活时间校验
        BigInteger hp_lefttime_bi = new BigInteger(hp_lefttime);
        if (  hp_lefttime_bi.compareTo(hp_id_timeout_bi) < 0 || hp_lefttime_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ) {
            throw new HiveAuthzPluginException("鉴权连接池连接存在时间超过预期Long值");
        }

        //初始化连接池配置
        MysqlUtil.init(url,username,password,hp_maxsize_bi.intValue(),hp_minidle_bi.intValue(),hp_id_timeout_bi.longValue(),hp_lefttime_bi.longValue());

        return new MyHiveAuthorization(hiveMetastoreClientFactory,hiveConf,hiveAuthenticationProvider,hiveAuthzSessionContext);
    }
}
