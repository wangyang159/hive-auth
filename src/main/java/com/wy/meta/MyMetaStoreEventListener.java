package com.wy.meta;

import com.wy.utils.FieldDiff;
import com.wy.utils.MysqlUtil;
import com.wy.utils.UserUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.Iterator;
import java.util.List;

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

    //调用鉴权数据库的参数
    private final String url;
    private final String driver;
    private final Long timeout;
    private final String username;
    private final String password;
    private final int hp_maxsize;
    private final int hp_minidle;
    private final long hp_id_timeout;
    private final long hp_lefttime;

    public MyMetaStoreEventListener(Configuration config) {
        super(config);

        //初始化鉴权库的连接池
        url = config.get("hive.auth.database.url");
        driver = config.get("hive.auth.database.driver");
        username = config.get("hive.auth.database.username");
        password = config.get("hive.auth.database.password");

        //链接超时
        BigInteger timeout_bi = new BigInteger(config.get("hive.auth.database.timeout"));
        timeout = timeout_bi.longValue();

        //数据库连接池大小校验
        BigInteger hp_maxsize_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.maxsize"));
        hp_maxsize = hp_maxsize_bi.intValue();

        //数据库连接池空闲连接大小校验
        BigInteger hp_minidle_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.minidle"));
        hp_minidle = hp_minidle_bi.intValue();

        //数据库连接池空闲超时校验
        BigInteger hp_id_timeout_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.idle.timeout"));
        hp_id_timeout = hp_id_timeout_bi.longValue();

        //数据库连接池连接最大存活时间校验
        BigInteger hp_lefttime_bi = new BigInteger(config.get("hive.auth.database.hikari.pool.max.lifetime"));
        hp_lefttime = hp_lefttime_bi.longValue();

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

        /*
        下面就是按需将权限写入你的外部系统中，当然这只在需要给非owner权限的情况下
        应为owner的权限在鉴权时，使用了元数据服务中的owner做判断，当操作表的用户不是owner才会走鉴权库相关逻辑
        这里将表面写入鉴权库
         */
        //将外部权限库中的表、字段全系数据删除
        MysqlUtil mysqlUtil = new MysqlUtil(url,driver,timeout,username,password,1,0,hp_id_timeout,hp_lefttime);
        Connection connection=null;
        StringBuilder sql = new StringBuilder();
        try {
            connection = mysqlUtil.getConnection();
            CallableStatement callableStatement = connection.prepareCall("{call InsertTableInfo(?,?)}");
            callableStatement.setString(1,owner);
            callableStatement.setString(2,dbName+"."+tableName);
            ResultSet resultSet = callableStatement.executeQuery();
            resultSet.next();
            if (resultSet.getInt("result") == -1){
                LOGGER.info("鉴权库缺失用户数据，需联系引擎管理添加，但这不影响表的创建，只是鉴权数据owner为空 - 录入表: {} - 确实owner: {}",dbName+"."+tableName,owner);
                throw new MetaException("鉴权库缺失用户数据，需联系引擎管理添加，但这不影响表的创建，只是鉴权数据中owner为空 - 录入表: "+dbName+"."+tableName+" - 确实owner: "+owner);
            } else {
                LOGGER.info("表信息录入 表:{} owner:{}",dbName+"."+tableName,owner);
            }
        } catch (SQLException e) {
            throw new RuntimeException("清理表权限信息出现异常 - "+e.getMessage());
        } finally {
            mysqlUtil.closeConnection(connection);
            mysqlUtil.closePool();
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
        MysqlUtil mysqlUtil = new MysqlUtil(url,driver,timeout,username,password,1,0,hp_id_timeout,hp_lefttime);
        Connection connection=null;
        StringBuilder sql = new StringBuilder();
        try {
            connection = mysqlUtil.getConnection();
            CallableStatement callableStatement = connection.prepareCall("{call DeleteTableAndAuth(?)}");
            callableStatement.setString(1,dbName+"."+tableName);
            ResultSet resultSet = callableStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            if (metaData.getColumnCount() == 3){
                resultSet.next();
                LOGGER.info("权限回收 表:{} 回收权限个数:{} 回收表信息个数:{}",dbName+"."+tableName,resultSet.getInt("auth_records_deleted"),resultSet.getInt("info_records_deleted"));
            } else if (metaData.getColumnCount() == 2) {
                LOGGER.info("未发现可回收权限 表:{}",dbName+"."+tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException("清理表权限信息出现异常 - "+e.getMessage());
        } finally {
            mysqlUtil.closeConnection(connection);
            mysqlUtil.closePool();
        }
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        //如果本次事件不成功，就什么都不干
        if (!tableEvent.getStatus()){
            return;
        }

        Table oldtable = tableEvent.getOldTable();
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
          FieldDiff 类中预留有4组可区分的字段操作类型，按照需要自己更改生成逻辑即可
          在这里只做了最基本的，对删除字段权限回收
          在业内多数情况下会有很多丰富的权限需求，对于表结构的更改来讲
          就像元数据变动前置类中注释写的那样，普遍会有某些情况表是不允许改变的，当然这种情况下会在前置类中拦截
          而这里要说的是，对于外部鉴权系统普遍会有另行收集表结构的需求
          就如上面建表时写入最基本的表名和owner那样
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

        List<FieldSchema> addedFields = fieldDiff.addedFields;
        //对于判定为新增的字段，要做什么操作
        if (addedFields != null && !addedFields.isEmpty() && addedFields.size()!=0) {
            LOGGER.info("新增字段 {}",addedFields);
        }

        List<FieldSchema> deletedFields = fieldDiff.deletedFields;
        //对于判定为删除的字段，要做什么操作，通常是回收所以字段已经失效的权限数据
        if (deletedFields != null && !deletedFields.isEmpty() && deletedFields.size()!=0) {
            LOGGER.info("删除字段 {}",deletedFields);
            MysqlUtil mysqlUtil = new MysqlUtil(url,driver,timeout,username,password,1,0,hp_id_timeout,hp_lefttime);
            Connection connection = null;
            try {
                connection = mysqlUtil.getConnection();

                StringBuilder sql = new StringBuilder();
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
                throw new MetaException("回收删除字段权限出现异常");
            } finally {
                mysqlUtil.closeConnection(connection);
                mysqlUtil.closePool();
            }
        }

        //如果表存储位置发生改变，要做什么操作
        Table newtable = tableEvent.getNewTable();
        String oldtable_location = oldtable.getSd().getLocation();
        String newtable_location = newtable.getSd().getLocation();
        if ( ! oldtable_location.equals(newtable_location) ) {
            LOGGER.info("存储地址变更 {} -> {} ", oldtable_location, newtable_location);
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
            throw new MetaException("新增分区扫尾工作出现异常");
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
            throw new MetaException("删除分区扫尾工作出现异常");
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
