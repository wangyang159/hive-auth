package com.wy.auth;

import com.wy.utils.MysqlUtil;
import com.wy.utils.SqlFieldAuthCheckUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.*;
import org.apache.thrift.TException;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * hive 鉴权类
 * 这个类在 一个新的客户端或者hiveserver2连接后，提交第一个sql时 由工厂模式实例化，后面只要不断开连接会自动复用
 * 用来对用户的sql所操作的关联主体做鉴权操作
 */
public class MyHiveAuthorization implements HiveAuthorizer {
    //Log日志类
    public static final Log LOG = LogFactory.getLog(MyHiveAuthorization.class);

    //存储鉴权用到的元数据服务客户端生成对象、hive配置、用户身份
    private final HiveMetastoreClientFactory metastoreClientFactory;
    private final HiveConf hiveConf;
    private final HiveAuthenticationProvider hiveAuthProvider;
    private final HiveAuthzSessionContext hiveAuthzSessionContext;

    //留给创建鉴权库连接池的参数
    private final String url;
    private final String driver;
    private final Long timeout;
    private final String username;
    private final String password;
    private final int hp_maxsize;
    private final int hp_minidle;
    private final long hp_id_timeout;
    private final long hp_lefttime;

    /*
    准备一个自定义的全字段标识，按需来就行，但是这里后面没有具体使用
    只是留给写入权限也包含在外部系统的场景改造用的
    这里一来嫌麻烦，二来业内通常写入权限只给owner
    所以用hive元数据中的owner做判断了
     */
    private static final String INSERT_VALUE_TAG = "-";

    /**
     * 当前类的构造器
     * @param metastoreClientFactory 一个元数据服务相关操作的客户端获取工厂类
     * @param conf hive的配置对象
     * @param authenticator 可以用来获取当前用户的身份和所在组
     * @param ctx  这个是任务会话的上下文对象，这个其实没啥用，只能输出当前连接来源于客户端还是hiveserver2
     * @throws HiveAuthzPluginException
     */
    public MyHiveAuthorization(HiveMetastoreClientFactory metastoreClientFactory,
                               HiveConf conf, HiveAuthenticationProvider authenticator,
                               HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {

        this.metastoreClientFactory = metastoreClientFactory;
        this.hiveConf = conf;
        this.hiveAuthProvider = authenticator;
        this.hiveAuthzSessionContext = ctx;

        /*
        初始化鉴权库的连接池参数

        此外，这里留一个关键注释：后面用到鉴权库连接池的地方，都会额外读取一遍配置
        因为写成在统一的MysqlUtil中会导致有的后面触发的元数据安全类拿不到这些配置从而变成了Null
        所以干脆获取值就各各写个的了

        不过，对于数值类型的配置，只在这里做校验后面使用的类，直接获取到就转换了，不再做重复校验
        因为 MyHiveAuthorization 类会相较于另外两个在执行时往往优先被吊起
         */
        url = hiveConf.get("hive.auth.database.url");
        driver = hiveConf.get("hive.auth.database.driver");
        username = hiveConf.get("hive.auth.database.username");
        password = hiveConf.get("hive.auth.database.password");

        //链接超时
        BigInteger timeout_bi = new BigInteger(hiveConf.get("hive.auth.database.timeout"));
        if ( timeout_bi.compareTo(BigInteger.valueOf(0)) < 0 || timeout_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0   ){
            throw new HiveAuthzPluginException("鉴权连接池连接超时时间超过预期Long值");
        }
        timeout = timeout_bi.longValue();

        //数据库连接池大小校验
        BigInteger hp_maxsize_bi = new BigInteger(hiveConf.get("hive.auth.database.hikari.pool.maxsize"));
        if ( hp_maxsize_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_maxsize_bi.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0 ){
            throw new HiveAuthzPluginException("鉴权连接池大小超过预期Int值");
        }
        hp_maxsize = hp_maxsize_bi.intValue();

        //数据库连接池空闲连接大小校验
        BigInteger hp_minidle_bi = new BigInteger(hiveConf.get("hive.auth.database.hikari.pool.minidle"));
        if ( hp_minidle_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_minidle_bi.compareTo(BigInteger.valueOf( hp_maxsize_bi.intValue() / 2 )) > 0   ){
            throw new HiveAuthzPluginException("鉴权连接池空闲连接大小超过预期Int值");
        }
        hp_minidle = hp_minidle_bi.intValue();

        //数据库连接池空闲超时校验
        BigInteger hp_id_timeout_bi = new BigInteger(hiveConf.get("hive.auth.database.hikari.pool.idle.timeout"));
        if ( hp_id_timeout_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_id_timeout_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0  ){
            throw new HiveAuthzPluginException("鉴权连接池空闲超时时间超过预期Long值");
        }
        hp_id_timeout = hp_id_timeout_bi.longValue();

        //数据库连接池连接最大存活时间校验
        BigInteger hp_lefttime_bi = new BigInteger(hiveConf.get("hive.auth.database.hikari.pool.max.lifetime"));
        if (  hp_lefttime_bi.compareTo(hp_id_timeout_bi) < 0 || hp_lefttime_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ) {
            throw new HiveAuthzPluginException("鉴权连接池连接存在时间超过预期Long值");
        }
        hp_lefttime = hp_lefttime_bi.longValue();

        //这里用System输出，而不用日志类，是因为该类会被工厂模式实例化构建时日志类还没有生效
        System.out.println("Hive Authz Plugin Initialized! 鉴权组件接入! ");
    }

    /**
     * 鉴权时被调用的方法
     *
     * 注意！！鉴权方法生效于计算任务开始之前！只有过了鉴权MR任务才开始被生成
     *
     * @param hiveOpType 当前操作的类型，是一个枚举类，注意当方法被调用时，该对象标识的时截至当前任务阶段的当前操作实在干什么
     *                   比如 insert 操作，鉴权时该对象只是查询，至于要对上下游干什么，上下游是谁，要分别通过下面两个对象去判断
     * @param inputHObjs 当前操作涉及的上游对象
     * @param outputHObjs 当前操作涉及的下游对象
     * @param context hive会话的上下文
     * @throws HiveAuthzPluginException 鉴权方法的判断模式，是当前方法不抛出异常则鉴权通过
     */
    @Override
    public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
                                List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context) throws HiveAuthzPluginException, HiveAccessControlException {

        LOG.info("当前操作类型是：" + hiveOpType + " 用户SQL为："+context.getCommandString() + " 用户身份："+hiveAuthProvider.getUserName());

        //需要的临时变量
        IMetaStoreClient metastoreClient = null;
        Table table = null;

        //对 查询集 和 结果集建表 做的鉴权操作
        if (hiveOpType == HiveOperationType.QUERY || hiveOpType == HiveOperationType.CREATETABLE_AS_SELECT) {
            //后面要鉴权的字段集合
            Map<String, List<String>> checkPrivilegeObject = new HashMap<>();
            //获取一个元数据连接
            metastoreClient = metastoreClientFactory.getHiveMetastoreClient();

            /*
            当操作是 SQL类型是查询，且此时操作涉及的上、下游对象不为空
            同时对下游对象要做的操作是插入数据，则获取下游表的信息，此时下游表其实只有一张

            在hive中hiveOpType代表用户提上来的SQL操作的初步判断
            HivePrivObjectActionType是获取对于权限对象的准确操作
            使用时，要一起判断才能准确的把控当前实在干什么
            如果只用其中一个，那么会有越过权限检测的情况发生
            不同的hive版本，最好是自己测试一下不同操作触发的枚举类
            hive3.1.3中insert as select语句 HiveOperationType 是 QUERY
             */
            if(hiveOpType == HiveOperationType.QUERY && inputHObjs.isEmpty() && !outputHObjs.isEmpty()
                    && (  outputHObjs.get(0).getActionType() == HivePrivilegeObject.HivePrivObjectActionType.INSERT_OVERWRITE
                    || outputHObjs.get(0).getActionType() == HivePrivilegeObject.HivePrivObjectActionType.INSERT)){

                //获取下游表的 库、表名，由于是插入操作所以字段直接给默认的全字段标识
                for(HivePrivilegeObject outputHObj: outputHObjs){
                    String dbName = outputHObj.getDbname();
                    String tblName = outputHObj.getObjectName();
                    try {
                        table = metastoreClient.getTable(dbName, tblName);
                    } catch (TException e) {
                        throw new HiveAuthzPluginException("字段鉴权，写入表对象获取失败 ",e);
                    }

                    if ( !table.getOwner().equals(hiveAuthProvider.getUserName()) ){
                        throw new HiveAuthzPluginException("只有表owner才可以写表");
                    }

                    //List<String> checkfieldList = new ArrayList<>();
                    //默认的全字段标识
                    //checkfieldList.add(INSERT_VALUE_TAG);
                    //checkPrivilegeObject.put(dbName + "." + tblName, checkfieldList);
                    LOG.info("检测到数据落盘表： " + dbName + "." + tblName + " , 操作落盘");
                }
            }

            //下游的信息拿到之后，这里还要拿到上游的数据来源表
            for (HivePrivilegeObject inputHObj : inputHObjs) {

                //拿出查询字段集合
                List<String> checkfieldList = inputHObj.getColumns();

                if ( checkfieldList == null || checkfieldList.isEmpty() ) {
                    /*
                    着重解释一下这里的操作非法
                    hive的鉴权用到的 inputHObj 是sql操作的对象，它依赖于hive对sql的语法识别
                    如果不对字段为空做判断，此时就会由一种常见的特殊情况绕过字段检查
                    也就是当 用户的sql中没有明确的访问字段时，getColumns返回的是一个空集合

                    例如：select count(1) as cnt from default.a
                        用户 A 提交上面的sql    查询用户 B 的 default.a 表
                        此时检测到的字段会是个空集合

                    当然实际使用中如果允许这种情况就不用这样写，只是在本例子中，下面会判断字段是否为空，为空时不会做鉴权

                     */
                    throw new HiveAuthzPluginException("字段鉴权,操作非法！不可跳过字段间接访问！");
                }

                String dbName = inputHObj.getDbname();
                String tblName = inputHObj.getObjectName();
                try {
                    //通过元数据连接获取这个表
                    table = metastoreClient.getTable(dbName, tblName);
                    //成功获取到表对象，且表不是临时表，获取到的字段也不为空，当前用户不是查询表的owner，则放到要鉴权的表信息集合中
                    //isTemporary判断是否是临时表，临时表只在一次session中存在，元数据不存在元数据服务中而是在内存中，比如你with as 的临时表这种
                    if (null != table && !table.isTemporary() && null != checkfieldList && !checkfieldList.isEmpty() && !table.getOwner().equals(hiveAuthProvider.getUserName())) {
                        checkPrivilegeObject.put(dbName + "." + tblName, checkfieldList);
                    }
                } catch (TException e) {
                    throw new HiveAuthzPluginException("字段鉴权 , 获取表信息失败. ms:" + e.getMessage());
                }
                LOG.info("检测到查询访问来自于目的表： " + dbName + "." + tblName + " , 访问字段：" + checkfieldList);
            }

            //上面需要元数据连接的地方走完，这里关闭元数据连接
            metastoreClient.close();

            //如果要鉴权的集合不是空的，则开始鉴权
            if (checkPrivilegeObject.size() != 0) {
                LOG.info("开始鉴权");
                try {
                    //程序触发checkPermissions方法后不会有 0 的情况，这里只是给个默认值
                    int task_count = 0;
                    if (checkPrivilegeObject.size() >= hp_maxsize) {
                        task_count = hp_maxsize;
                    } else {
                        task_count = checkPrivilegeObject.size();
                    }

                    // 创建定长线程池 , 线程池长度  与 数据库连接池的大小 保持一致，这样每个任务都能那个一个连接
                    ExecutorService executor = Executors.newFixedThreadPool(task_count);
                    MysqlUtil mysqlUtil = new MysqlUtil(url,driver,timeout,username,password,task_count,0,hp_id_timeout,hp_lefttime);

                    new SqlFieldAuthCheckUtil().checkPermissions(checkPrivilegeObject,hiveAuthProvider.getUserName(),executor,mysqlUtil);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        } else if ( hiveOpType == HiveOperationType.GRANT_PRIVILEGE || hiveOpType == HiveOperationType.REVOKE_PRIVILEGE
                    || hiveOpType == HiveOperationType.REVOKE_ROLE || hiveOpType == HiveOperationType.GRANT_ROLE
                    || hiveOpType == HiveOperationType.SHOW_GRANT || hiveOpType == HiveOperationType.SHOW_ROLE_GRANT
                    || hiveOpType == HiveOperationType.SHOW_ROLE_PRINCIPALS || hiveOpType == HiveOperationType.CREATEROLE
                    || hiveOpType == HiveOperationType.SHOW_ROLES) {
            //由于是自定义权限，所以与之相关的语句不再允许执行
            throw new HiveAuthzPluginException("不支持原生Hive赋权操作");

        } else if (hiveOpType == HiveOperationType.TRUNCATETABLE ) {
            //只有表 owner 才有表的写相关的权限
            String dbName = outputHObjs.get(0).getDbname();
            String tblName = outputHObjs.get(0).getObjectName();
            try {
                metastoreClient = metastoreClientFactory.getHiveMetastoreClient();
                table = metastoreClient.getTable(dbName, tblName);
                metastoreClient.close();
                if ( !hiveAuthProvider.getUserName().equals( table.getOwner() )){
                    throw new HiveAuthzPluginException("清空表数据需要owner权限");
                }
            } catch (TException e) {
                throw new HiveAuthzPluginException("鉴权 , 获取表信息失败. ms:" + e.getMessage());
            }

        } else if (hiveOpType == HiveOperationType.DESCTABLE) {
            String dbName = inputHObjs.get(0).getDbname();
            String tblName = inputHObjs.get(0).getObjectName();
            try {
                metastoreClient = metastoreClientFactory.getHiveMetastoreClient();
                table = metastoreClient.getTable(dbName, tblName);
                metastoreClient.close();
                if ( !hiveAuthProvider.getUserName().equals( table.getOwner() ) ){
                    throw new HiveAuthzPluginException("展示表详情信息数据需要owner权限");
                }
            } catch (TException e) {
                throw new HiveAuthzPluginException("鉴权 , 获取表信息失败. ms:" + e.getMessage());
            }

        } else {
            //关闭元数据连接
            if (metastoreClient != null) {
                metastoreClient.close();
            }

        }
    }


    /**
     * 这个方法用来获取当前权限控制器的版本
     * 这里的版本指的是，在其他地方调用这个自定义权限插件时，用来做区分的
     * 一般没啥用，hive自带源码中只有一个 V1
     * 要是有区分需要把 org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer 源码中的版本枚举类加一个这里调用就行
     */
    @Override
    public VERSION getVersion() {
        LOG.info("getVersion 被触发");
        return null;
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void grantPrivileges(List<HivePrincipal> list, List<HivePrivilege> list1, HivePrivilegeObject hivePrivilegeObject, HivePrincipal hivePrincipal, boolean b) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("grantPrivileges 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void revokePrivileges(List<HivePrincipal> list, List<HivePrivilege> list1, HivePrivilegeObject hivePrivilegeObject, HivePrincipal hivePrincipal, boolean b) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("revokePrivileges 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void createRole(String s, HivePrincipal hivePrincipal) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("createRole 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void dropRole(String s) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("dropRole 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String s) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("getPrincipalGrantInfoForRole 被触发");
        return Collections.emptyList();
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal hivePrincipal) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("getRoleGrantInfoForPrincipal 被触发");
        return Collections.emptyList();
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void grantRole(List<HivePrincipal> list, List<String> list1, boolean b, HivePrincipal hivePrincipal) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("grantRole 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void revokeRole(List<HivePrincipal> list, List<String> list1, boolean b, HivePrincipal hivePrincipal) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("revokeRole 被触发");
    }

    /**
     * 展示资源列表的时候触发，比如运行了 show tablse 这种
     * 可以在这里完成有某些权限的展示过滤
     */
    @Override
    public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> list, HiveAuthzContext hiveAuthzContext) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("filterListCmdObjects 被触发，入参：" + list);
        return list;
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("getAllRoles 被触发");
        return Collections.emptyList();
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public List<HivePrivilegeInfo> showPrivileges(HivePrincipal hivePrincipal, HivePrivilegeObject hivePrivilegeObject) throws HiveAuthzPluginException, HiveAccessControlException {
        LOG.info("showPrivileges 被触发");
        return Collections.emptyList();
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public void setCurrentRole(String s) throws HiveAccessControlException, HiveAuthzPluginException {
        LOG.info("setCurrentRole 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
        LOG.info("getCurrentRoleNames 被触发");
        return Collections.emptyList();
    }

    /**
     * 在鉴权组件初始化之后，也就是一个新的会话连接之后，会被调用，传入当前hive配置，来做一些操作
     * 按需使用即可，比如起到一个检查配置的作用
     * 或者连接的用户必须携带某些配置，如身份认证等
     *
     */
    @Override
    public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
        LOG.info("applyAuthorizationConfigPolicy 被触发");
    }

    /**
     * 屏蔽原生hive赋权语句后，此方法无用
     */
    @Override
    public Object getHiveAuthorizationTranslator() throws HiveAuthzPluginException {
        LOG.info("getHiveAuthorizationTranslator 被触发");
        return null;
    }

    /**
     *
     * 和org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizationValidator一样保持空就行
     */
    @Override
    public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext hiveAuthzContext, List<HivePrivilegeObject> list) throws SemanticException {
        LOG.info("applyRowFilterAndColumnMasking 被触发 ， 入参"+list);
        return Collections.emptyList();
    }

    /**
     * 这个方法截至hive3.1.3没有大用，默认给false就行，往后的版本需要看源码
     * 在hive默认权限类没这个方法
     * 自带的SQL标准化插件 org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizationValidator 中也是 false
     * @return
     */
    @Override
    public boolean needTransform() {
        LOG.info("needTransform 被触发");
        return false;
    }

    /**
     * 这个方法返回 一个 HivePolicyProvider 实例，在每一个sql执行前被调用一次,包括hiveserver2启动之后也会调用一次
     * 验证访问资源的ACL权限
     * 通常不会直接使用这个，而是在上面的鉴权方法中写细化的逻辑
     * 因为这个方法偏原生架构，用户、组、角色这种鉴权逻辑，对于高自主实现的场景不太好用
     * @return
     * @throws HiveAuthzPluginException
     */
    @Override
    public HivePolicyProvider getHivePolicyProvider() throws HiveAuthzPluginException {
        LOG.info("getHivePolicyProvider 被触发");
        return null;
    }
}
