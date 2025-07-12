package com.wy.meta;

import com.wy.exception.HiveMetaStoreException;
import com.wy.utils.FieldDiff;
import com.wy.utils.MysqlUtil;
import com.wy.utils.UserUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * 这个类在hive元数据的操作完成之后触发，注意是完成之后！！干什么，起到一个回调的作用
 * 有别于 MyHiveAuthorization 类，MyMetaStoreEventListener 是基于元数据的相关策略
 * 而 MyHiveAuthorization 是基于用户SQL主观上访问主体的安全策略
 *
 * 要注意的是 超类 MetaStoreEventListener 提供了较多的方法来实现元数据被操作之后干什么
 * 方法名很直白，一看就知道是干什么的，所以就不一一列举了
 * 本例中只监控 最 最 最 最重要的 表、分区 被更改时做的操作
 *
 */
public class MyMetaStoreEventListener extends MetaStoreEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyMetaStoreEventListener.class);

    private MysqlUtil mysqlUtil;
    //新增表信息用的sql
    String insertTbInfo = "{call InsertTableInfo(?,?,?,?)}";
    //删除表信息用的sql
    String dropTbInfo = "{call DeleteTableAndAuth(?)}";
    //更新表用的sql
    String updateTbInfo = "update db_tb_info set tb_fields = ? where db_tb_name=? ";

    public MyMetaStoreEventListener(Configuration config) throws HiveMetaStoreException {
        super(config);

        //初始化来连接池和线程池的配置参数
        String url;
        String driver;
        Long timeout;
        String username;
        String password;
        int hp_maxsize;
        int hp_minidle;
        long hp_id_timeout;
        long hp_lefttime;

        //初始化鉴权库的连接池
        url = config.get("hive.auth.database.url");
        driver = config.get("hive.auth.database.driver");
        username = config.get("hive.auth.database.username");
        password = config.get("hive.auth.database.password");

        //数据库连接超时时间
        BigInteger timeout_bi = new BigInteger(config.get("hive.auth.database.timeout"));
        if ( timeout_bi.compareTo(BigInteger.valueOf(0)) < 0 || timeout_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0   ){
            throw new HiveMetaStoreException("鉴权连接池连接超时时间超过预期Long值");
        }
        timeout = timeout_bi.longValue();

        //数据库连接池大小校验
        BigInteger hp_maxsize_bi = new BigInteger(config.get("hive.auth.database.meta.listener.hikari.pool.maxsize"));
        if ( hp_maxsize_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_maxsize_bi.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0 ){
            throw new HiveMetaStoreException("鉴权连接池大小超过预期Int值");
        }
        hp_maxsize = hp_maxsize_bi.intValue();

        //数据库连接池空闲连接大小校验
        BigInteger hp_minidle_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.minidle"));
        if ( hp_minidle_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_minidle_bi.compareTo(BigInteger.valueOf( hp_maxsize_bi.intValue() / 2 )) > 0   ){
            throw new HiveMetaStoreException("鉴权连接池空闲连接大小超过预期Int值");
        }
        hp_minidle = hp_minidle_bi.intValue();

        //数据库连接池空闲超时校验
        BigInteger hp_id_timeout_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.idle.timeout"));
        if ( hp_id_timeout_bi.compareTo(BigInteger.valueOf(0)) < 0 || hp_id_timeout_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0  ){
            throw new HiveMetaStoreException("鉴权连接池空闲超时时间超过预期Long值");
        }
        hp_id_timeout = hp_id_timeout_bi.longValue();

        //数据库连接池连接最大存活时间校验
        BigInteger hp_lefttime_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.max.lifetime"));
        if (  hp_lefttime_bi.compareTo(hp_id_timeout_bi) < 0 || hp_lefttime_bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ) {
            throw new HiveMetaStoreException("鉴权连接池连接存在时间超过预期Long值");
        }
        hp_lefttime = hp_lefttime_bi.longValue();

        mysqlUtil = new MysqlUtil(url,driver,timeout,username,password,hp_maxsize,hp_minidle,hp_id_timeout,hp_lefttime);

        System.out.println("Hive MetaStore Plugin Initialized! 元数据监控组件接入! ");
    }

    /**
     * 建表的时候，对表owner赋予 rw 权限
     * 你可以在此做一些其他的检测
     * @param tableEvent
     * @throws MetaException
     */
    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!tableEvent.getStatus()){
            return;
        }

        //通过入参对象获取表信息
        Table table = tableEvent.getTable();
        String dbName = table.getDbName();
        String tableName = table.getTableName();
        String location = table.getSd().getLocation();
        String tableType = table.getTableType();
        String owner = table.getOwner();

        //如果表路径为空 获取应当的默认location
        if (location == null || location.isEmpty()) {
            Database db;
            try {
                db = tableEvent.getHandler().get_database(dbName);
            } catch (TException e) {
                throw new MetaException("库信息获取失败 db:" +dbName + " table:" + tableName);
            }
            //此处均按照表在库下判断，所以第三个参数 isExternal 只传false
            location = String.valueOf(tableEvent.getHandler().getWh().getDefaultTablePath(db,tableName,false));
        }

        //获取用户身份，也就是owner
        String userName = UserUtil.getUserName();

        LOGGER.info("---------------------");
        LOGGER.info("|操作是      ：CreateTable");
        LOGGER.info("|库名        ： " + dbName);
        LOGGER.info("|表名        ： " + tableName);
        LOGGER.info("|表location  ： " + location);
        LOGGER.info("|表类型       ： " + tableType);
        LOGGER.info("|owner是       ： " + owner);
        LOGGER.info("|当前执行用户是       ： " + userName);
        LOGGER.info("---------------------");

        //下面就是按需将表信息按需写入鉴权库中
        Connection connection=null;
        try {
            connection = mysqlUtil.getConnection(false);
            CallableStatement callableStatement = connection.prepareCall(insertTbInfo);
            callableStatement.setString(1,owner);
            callableStatement.setString(2,dbName+"."+tableName);
            //处理表字段列表
            Stream<String> cols = table.getSd().getCols().stream().map(col -> col.getName());
            Stream<String> pars = table.getPartitionKeys().stream().map(par -> par.getName());
            StringBuilder tmp = new StringBuilder();
            cols.forEach(col -> {tmp.append(col).append(",");});
            pars.forEach(par -> {tmp.append(par).append(",");});
            tmp.deleteCharAt(tmp.length()-1);
            callableStatement.setString(3,tmp.toString());
            //表路径
            callableStatement.setString(4,location);
            ResultSet resultSet = callableStatement.executeQuery();
            resultSet.next();
            if (resultSet.getInt("result") == -1){
                throw new MetaException("鉴权库缺失用户数据，已经自动触发添加，不影响表的创建，但请联系管理员告知这种意外 - 录入表: "+dbName+"."+tableName+" - 补充的owner: "+owner);
            } else {
                LOGGER.info("表信息录入成功 表:{} owner:{} 字段:{}",dbName+"."+tableName,owner,tmp.toString());
            }
        } catch (SQLException e) {
            throw new MetaException("录入表信息出现异常 - "+e.getMessage());
        } finally {
            mysqlUtil.closeConnection(connection);
        }
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!tableEvent.getStatus()){
            return;
        }

        //通过入参对象获取表信息
        Table table = tableEvent.getTable();
        String dbName = table.getDbName();
        String tableName = table.getTableName();
        String location = table.getSd().getLocation();
        String tableType = table.getTableType();
        String owner = table.getOwner();

        //获取用户身份
        String userName = UserUtil.getUserName();

        LOGGER.info("---------------------");
        LOGGER.info("|操作是      ：DropTable");
        LOGGER.info("|库名        ： " + dbName);
        LOGGER.info("|表名        ： " + tableName);
        LOGGER.info("|表location  ： " + location);
        LOGGER.info("|表类型       ： " + tableType);
        LOGGER.info("|owner是       ： " + owner);
        LOGGER.info("|当前执行用户是       ： " + userName);
        LOGGER.info("---------------------");

        //将外部权限库中的表、字段全系数据删除
        Connection connection=null;
        try {
            connection = mysqlUtil.getConnection(false);
            CallableStatement callableStatement = connection.prepareCall(dropTbInfo);
            callableStatement.setString(1,dbName+"."+tableName);
            ResultSet resultSet = callableStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            resultSet.next();
            if (metaData.getColumnCount() == 3){
                LOGGER.info("权限回收 {} {} {}",resultSet.getString("table_name"),resultSet.getString("auth_records_deleted"),resultSet.getString("info_records_deleted"));
            } else if (metaData.getColumnCount() == 2) {
                LOGGER.info("{} 表:{}",resultSet.getString("message"),resultSet.getString("table_name"));
            }
        } catch (SQLException e) {
            throw new MetaException("清理表权限信息出现异常 - "+e.getMessage());
        } finally {
            mysqlUtil.closeConnection(connection);
        }
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!tableEvent.getStatus()){
            return;
        }

        Table oldtable = tableEvent.getOldTable();
        Table newTable = tableEvent.getNewTable();
        String tableType = oldtable.getTableType();

        //视图不需要做操作
        if ("MATERIALIZED_VIEW".equals(tableType) || "VIRTUAL_VIEW".equals(tableType))
            return;

        //获取用户身份
        String userName = UserUtil.getUserName();

        LOGGER.info("---------------------");
        LOGGER.info("|操作是       ： AlterTable");
        LOGGER.info("|表类型       ： " + tableType);
        LOGGER.info("|用户是       ： " + userName);
        LOGGER.info("---------------------");

        /*
         这里留一个关键注释：
          FieldDiff 类中有4组可区分的字段操作类型，按照需要自己更改生成逻辑即可
          在这里只做了最基本的，对删除字段权限回收，以及即使的更新鉴权库中的字段列表

          而在业内多数情况下会有很多丰富的权限需求，对于表结构的更改来讲
          就像元数据变动前置类中注释写的那样，普遍会有某些情况表是不允许改变的
          当然这种情况下会在前置类中拦截

          而这里要说的是，对于外部鉴权系统普遍都是另行收集表结构
          就如上面建表时写入最基本的表名、owner、字段列那样
          虽然这些数据在hive元数据库中都有，但是为了可扩展性，以及hive元数据库自身的安全以及稳定来讲
          基本不可能让其他自定义的功能直接访问hive元数据服务的库去，因此这就看具体使用时方案怎么决定了

          至于hive元数据服务的mysql中，如果真的需要获取，表相关的信息储存在下面的表中
          DBS：存放了库信息
              关键字段：DB_ID 库的唯一id
                      DB_LOCATION_URI 库的默认储存地址
                      NAME 库名
                      DESC 库的说明

          TBLS：存放了表信息
               关键字段：TBL_ID 表的唯一id
                       DB_ID 库的唯一id
                       SD_ID  表描述符id ， 也就是表存储类型、数据分隔符那些
                       OWNER 表owner的名称
                       TBL_NAME 表名
                       TBL_TYPE 表类型 - MANAGED_TABLE(内), EXTERNAL_TABLE(外)

           COLUMNS_V2：存放了表的列信息，0.14之前的hive在COLUMNS里面
                关键字段：CD_ID 列数据的所属id，这个字段使用时注意它本身是 CDS 表的外键，同一个表的该字段数据是重复的，而 CDS表 只有一列，存储的是表信息的ID
                        COMMENT 列描述
                        COLUMN_NAME 列名
                        TYPE_NAME 类型
                        INTEGER_IDX 字段在表中的顺序，从0开始，这个顺序在深度开发hive插件时相当重要

           SDS：存放了表描述符信息
                关键字段：SD_ID 描述符id
                        CD_ID 表信息id
                        INPUT_FORMAT  输入格式，比如org.apache.hadoop.mapred.TextInputFormat
                        OUTPUT_FORMAT 输出格式
                        LOCATION 表存放位置
                        IS_COMPRESSED 是否启用压缩， 0 启用
                        IS_STOREDASSUBDIR  是否按子目录储存，0 是，一般都是 0
                        NUM_BUCKETS 若为分桶表则这里表示数量，如果不是则默认 -1
                        SERDE_ID  系列化类型的id 是 SERDES表的外键，但是SERDES表除了系列化类名称之外其他一般都是空的
         */
        FieldDiff fieldDiff = new FieldDiff(tableEvent);
        boolean changed = false;//是否发生改变的改变的标识，不用fieldDiff的为更改是因为它不包含删除字段

        List<FieldSchema> addedFields = fieldDiff.addedFields;
        //对于判定为新增的字段，要做什么操作
        if (addedFields != null && !addedFields.isEmpty() && addedFields.size()!=0) {
            changed = true;
            LOGGER.info("新增字段 {}",addedFields);
        }

        List<FieldSchema> deletedFields = fieldDiff.deletedFields;
        // 对于判定为删除的字段，要做什么操作 通常是回收已经失效的权限数据
        if (deletedFields != null && !deletedFields.isEmpty() && deletedFields.size()!=0) {
            changed = true;
            LOGGER.info("删除字段 {}",deletedFields);
            Connection connection = null;
            try {
                connection = mysqlUtil.getConnection(false);
                StringBuilder sql = new StringBuilder();
                //这个语句中 in 部分用占位符会出问题，所以反正要拼接就没有提升sql主体的作用域
                sql.append("delete from db_tb_auth where db_tb_id in ")
                        .append("(select db_tb_id from db_tb_info where db_tb_name='")
                        .append(oldtable.getDbName()).append(".").append(oldtable.getTableName())
                        .append("') and field in (");
                //遍历需要删除的字段
                for (int i = 0; i < deletedFields.size(); i++) {
                    sql.append("'").append(deletedFields.get(i).getName()).append("',");
                }
                sql.delete(sql.length()-1,sql.length());
                sql.append(")");

                int delete_auth = connection.prepareStatement(sql.toString()).executeUpdate();

                if ( delete_auth != 0 ){
                    LOGGER.info("回收字段权限个数: {}",delete_auth);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new MetaException("回收删除字段权限出现异常"+e.getMessage());
            } finally {
                mysqlUtil.closeConnection(connection);
            }
        }

        // 如果上面发生了字段的删除或者新增则要更新鉴权库中的表字段
        // 后期改造要注意是否要考虑fieldDiff中旧字段发生改变的逻辑
        if (changed) {
            Connection connection = null;
            try {
                connection = mysqlUtil.getConnection(false);

                PreparedStatement preparedStatement = connection.prepareStatement(updateTbInfo);
                preparedStatement.setString(1, fieldDiff.fieldNames);
                preparedStatement.setString(2,newTable.getDbName()+"."+newTable.getTableName());
                int i = preparedStatement.executeUpdate();
                if ( i!=0 ){
                    LOGGER.info("更新表信息字段列表，数据库返回行数: {}",i);
                }
            } catch (SQLException e) {
                throw new MetaException("更新字段列表出现错误 "+e.getMessage());
            } finally {
                mysqlUtil.closeConnection(connection);
            }
        }

        //等等。。其他的情况
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!partitionEvent.getStatus()){
            return;
        }

        try {
            Table table = partitionEvent.getTable();
            Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
            while(partitionIterator.hasNext()){
                LOGGER.info("新增分区 {}.{} {}",table.getDbName(),table.getTableName(),partitionIterator.next().getValues());
            }
        }catch (Exception e){
            throw new MetaException("新增分区扫尾工作出现异常"+e.getMessage());
        }
    }

    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!partitionEvent.getStatus()){
            return;
        }

        try {
            Table table = partitionEvent.getTable();
            Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
            while(partitionIterator.hasNext()){
                LOGGER.info("删除分区 {}.{} {}",table.getDbName(),table.getTableName(),partitionIterator.next().getValues());
            }
        }catch (Exception e){
            throw new MetaException("删除分区扫尾工作出现异常"+e.getMessage());
        }
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!partitionEvent.getStatus()){
            return;
        }

        Table table = partitionEvent.getTable();
        Partition newPartition = partitionEvent.getNewPartition();
        Partition oldPartition = partitionEvent.getOldPartition();
        LOGGER.info("修改分区 {}.{} {} -> {}",table.getDbName(),table.getTableName(),oldPartition.getValues(),newPartition.getValues());
    }

}
