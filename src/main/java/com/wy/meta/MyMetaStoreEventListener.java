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
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public MyMetaStoreEventListener(Configuration config) {
        super(config);
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
         */
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
        MysqlUtil mysqlUtil = new MysqlUtil(1, 0);
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
            e.printStackTrace();
            throw new MetaException("清理表权限信息出现异常");
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

        Table newtable = tableEvent.getNewTable();

        //获取用户身份
        String userName = UserUtil.getUserName();

        LOGGER.info("---------------------");
        LOGGER.info("|操作是       ： AlterTable");
        LOGGER.info("|表类型       ： " + tableType);
        LOGGER.info("|用户是       ： " + userName);
        LOGGER.info("---------------------");

        FieldDiff fieldDiff = new FieldDiff(tableEvent);

        List<FieldSchema> addedFields = fieldDiff.addedFields;
        //对于判定为新增的字段，要做什么操作
        if (addedFields != null && !addedFields.isEmpty() && addedFields.size()!=0) {
            LOGGER.info("新增字段 {}",addedFields);
        }

        List<FieldSchema> deletedFields = fieldDiff.deletedFields;
        //对于判定为删除的字段，要做什么操作，通常是删除权限记录
        if (deletedFields != null && !deletedFields.isEmpty() && deletedFields.size()!=0) {
            LOGGER.info("删除字段 {}",deletedFields);
        }

        //如果表存储位置发生改变，要做什么操作
        String oldtable_location = oldtable.getSd().getLocation();
        String newtable_location = newtable.getSd().getLocation();
        if ( ! oldtable_location.equals(newtable_location) ) {
            LOGGER.info("存储地址变更 {} -> {} ", oldtable_location, newtable_location);
        }
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        try {
            Table table = partitionEvent.getTable();
            Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
            while(partitionIterator.hasNext()){
                LOGGER.info("新增分区 {}.{} {}",table.getDbName(),table.getTableName(),partitionIterator.next().getValues());
            }
        }catch (Exception e){
            LOGGER.error("error",e);
        }
    }

    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
        try {
            Table table = partitionEvent.getTable();
            Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
            while(partitionIterator.hasNext()){
                LOGGER.info("删除分区 {}.{} {}",table.getDbName(),table.getTableName(),partitionIterator.next().getValues());
            }
        }catch (Exception e){
            LOGGER.error("error",e);
        }
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
        Table table = partitionEvent.getTable();
        Partition newPartition = partitionEvent.getNewPartition();
        Partition oldPartition = partitionEvent.getOldPartition();
        LOGGER.info("修改分区 {}.{} {} -> {}",table.getDbName(),table.getTableName(),oldPartition.getValues(),newPartition.getValues());
    }


}
