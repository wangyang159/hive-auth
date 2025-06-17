package com.wy.auth;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.*;

public class MyHiveAuthorizationFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory hiveMetastoreClientFactory, HiveConf hiveConf, HiveAuthenticationProvider hiveAuthenticationProvider, HiveAuthzSessionContext hiveAuthzSessionContext) throws HiveAuthzPluginException {
        return new MyHiveAuthorization(hiveMetastoreClientFactory,hiveConf,hiveAuthenticationProvider,hiveAuthzSessionContext);
    }
}
