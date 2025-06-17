package com.wy.meta;

import com.wy.utils.UserUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 这个类在元数据被访问之前触发
 */
public class MyMetaStorePreEventListener extends MetaStorePreEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyMetaStorePreEventListener.class);

    private static String META_ALLUXIO_ENABLE = "hive.metastore.part.alluxio.enable";

    public MyMetaStorePreEventListener(Configuration config) {
        super(config);
        System.out.println("Hive MetaStorePre Plugin Initialized! 元数据辅助组件接入! ");
    }

    @Override
    public void onEvent(PreEventContext preEventContext) throws MetaException  {
        //下面会用到的变量
        Database db = null;
        Table table = null;
        String dbName = null;
        String tableName = null;
        String location = null;
        String tableType = null;

        String userName = UserUtil.getUserName();
        LOGGER.info("用户身份 {} " , userName);

        //从上下文中，获取hive的配置信息，获取是否开启alluxio分流
        Boolean isPartAlluxioEnabled = preEventContext.getHandler().getConf().getBoolean(META_ALLUXIO_ENABLE,false);

        //获取操作类型做出不同的判断策略
        switch (preEventContext.getEventType()) {
            case ALTER_DATABASE:
                throw new MetaException("禁止修改库信息!");
            case CREATE_DATABASE:
                PreCreateDatabaseEvent preCreateDatabaseEvent = (PreCreateDatabaseEvent) preEventContext;
                location = preCreateDatabaseEvent.getDatabase().getLocationUri();

                //一般情况下不会没有默认的路径，除非是非法语句提交
                if (location == null)
                    throw new MetaException("建库必须指定location(hdfs).");

                break;
            case DROP_DATABASE:
                throw new MetaException("不允许删库!");
            case CREATE_TABLE:
                PreCreateTableEvent preCreateTableEvent = (PreCreateTableEvent) preEventContext;
                table = preCreateTableEvent.getTable();
                tableType = table.getTableType();
                //1、视图则不需处理
                if ("MATERIALIZED_VIEW".equals(tableType) || "VIRTUAL_VIEW".equals(tableType))
                    break;

                //2、获取相关信息后面用
                tableName = table.getTableName();
                dbName = table.getDbName();
                location = table.getSd().getLocation();
                //如果表路径为空 获取应当的默认location
                if (location == null || location.isEmpty()) {
                    try {
                        db = preCreateTableEvent.getHandler().get_database(dbName);
                    } catch (TException e) {
                        throw new MetaException("库信息获取失败 db:" +dbName + " table:" + tableName);
                    }
                    //此处均按照表在库下判断，所以第三个参数 isExternal 只传false
                    //注意这里只是预留了路径的校验，不要把它set进table，那可能会导致表恒定的变成外表
                    location = String.valueOf(preCreateTableEvent.getHandler().getWh().getDefaultTablePath(db,tableName,false));
                }

                //表路径的校验，有的地方地底层路径做了统一规划，不过要跳过Paimon表
                // getSd 表的存储描述 、getSerdeInfo系列化和反系列化内容、getSerializationLib系列化类库
                String serializationLib = table.getSd().getSerdeInfo().getSerializationLib();
                LOGGER.info("serializationLib:"+ serializationLib);
                if(!table.getSd().getSerdeInfo().getSerializationLib().isEmpty()
                        && !table.getSd().getSerdeInfo().getSerializationLib().equals("org.apache.paimon.hive.PaimonSerDe")){
                    //不是Paimon表 才进行校验，因为Paimon实际数据不存储在传统的hdfs中

                }

                //3、鉴权系统中表存在索引，因此要限制表全称和字段长度
                if ( tableName.length()+dbName.length() > 100 ){
                    throw new MetaException("表全称不能超过100个字符 db:" +dbName + " table:" + tableName);
                }
                for (int i = 0; i < table.getSd().getColsSize(); i++) {
                    if ( table.getSd().getCols().get(i).getName().length() > 50 ){
                        throw new MetaException("字段名称不能超过50个字符 db:" +dbName + " table:" + tableName + " field:"+ table.getSd().getCols().get(i).getName());
                    }
                }
                for (int i = 0; i < table.getPartitionKeysSize(); i++) {
                    if ( table.getPartitionKeys().get(i).getName().length() > 50 ){
                        throw new MetaException("分区字段名称不能超过50个字符 db:" +dbName + " table:" + tableName + " partition_field:"+ table.getPartitionKeys().get(i).getName());
                    }
                }

                //其他：按照你的需要还可以进行其他的校验，比如分区表要干什么，或者多引擎类型下为了数据正常识别分区字段应该是字符串类型等等

                break;
            case DROP_TABLE :
                PreDropTableEvent preDropTableEvent = (PreDropTableEvent) preEventContext;
                table = preDropTableEvent.getTable();
                dbName = table.getDbName();
                tableName = table.getTableName();

                //下面是删表时，权限校验。只能owner删除
                if ( !userName.equals(table.getOwner()) ){
                    throw new MetaException("没有owner权限");
                }

                break;
            case ALTER_TABLE:
                PreAlterTableEvent preAlterTableEvent = (PreAlterTableEvent) preEventContext;
                Table oldTable = preAlterTableEvent.getOldTable();
                dbName = oldTable.getDbName();
                tableName = oldTable.getTableName();
                tableType = oldTable.getTableType();

                Table newtable = preAlterTableEvent.getNewTable();

                //1、视图则不需处理
                if ("MATERIALIZED_VIEW".equals(tableType) || "VIRTUAL_VIEW".equals(tableType))
                    break;

                //2、大部分情况下，有的公司是不允许改某些特定情况下的表的
                //  比如这个表已经对外提供了，这种情况就要在这里调用其他API等方式判断

                //3、这里和删表一样，要校验owner权限
                if ( !userName.equals(oldTable.getOwner()) ){
                    throw new MetaException("没有owner权限");
                }

                //4、如果 owner 权限通过 ， 校验库名是否合法更改 ， 开源hive支持变更库，这一点按实际需求来，通常是一刀切，如果不是就要校验改后的库是否有权限
                if ( !dbName.equals(newtable.getDbName()) ){
                    throw new MetaException("不能变更库");
                }

                //5、通常要校验 location 相关，比如为了有的公司不允许分区表更改location
                if ( oldTable.getPartitionKeysSize() > 0 && !oldTable.getSd().getLocation().equals(newtable.getSd().getLocation()) ){
                    throw new MetaException("分区表不允许更改表路径");
                }

                //6、其他按需管控操作细节，就和元数据操作提交之后触发的监听中类似的逻辑，只不过这里偏向于权限，元数据操作之后的监听偏向于扫尾工作

                break;
            case ADD_PARTITION:
                PreAddPartitionEvent preAddPartitionEvent = (PreAddPartitionEvent) preEventContext;
                table = preAddPartitionEvent.getTable();
                location = table.getSd().getLocation();

                if ( "MATERIALIZED_VIEW".equals(table.getTableType()) || "VIRTUAL_VIEW".equals(table.getTableType()))
                    throw new MetaException("不能操作视图");

                //如果携带了分区文字，则效验是否在表路径下
                String finalLocation = table.getSd().getLocation();
                List<Partition> partitions = preAddPartitionEvent.getPartitions();
                for (int i = 0; i < partitions.size(); i++) {
                    if (partitions.get(i).getSd() != null && partitions.get(i).getSd().getLocation() != null && !partitions.get(i).getSd().getLocation().startsWith(finalLocation)){
                        throw new MetaException("分区的路径不能改为非表下路径");
                    }else{
                        //hive的语法中add语句只能指定统一的一个路径所以第一次符合规范后循环就可以结束了，后续发现特殊情况再说
                        break;
                    }
                }

                break;
            case DROP_PARTITION :
                PreDropPartitionEvent preDropPartitionEvent = (PreDropPartitionEvent) preEventContext;
                table = preDropPartitionEvent.getTable();
                dbName = table.getDbName();
                tableName = table.getTableName();

                //校验owner
                if ( !userName.equals(table.getOwner()) ){
                    throw new MetaException("没有owner权限");
                }

                //其他权限校验

                break;
            case ALTER_PARTITION:
                PreAlterPartitionEvent preAlterPartitionEvent = (PreAlterPartitionEvent) preEventContext;
                tableName = preAlterPartitionEvent.getTableName();
                dbName = preAlterPartitionEvent.getDbName();

                //在非alluxio meta中，拦截alluxio分区的DDL操作
                Partition newPart = preAlterPartitionEvent.getNewPartition();
                if(isPartAlluxioEnabled){
                    throw new MetaException(dbName+"."+tableName +" partition "+newPart.getValues()+" 不能被更改, 受限于 Alluxio");
                }

                try {
                    //preAlterPartitionEvent没有getTable方法，但是可以Handler获取
                    table = preAlterPartitionEvent.getHandler().get_table(dbName, tableName);
                    if (table.getSd().getLocation() == null || "MATERIALIZED_VIEW".equals(table.getTableType()) || "VIRTUAL_VIEW".equals(table.getTableType()))
                        throw new MetaException("不能操作视图，以及未知路径的表");

                } catch (TException e) {
                    throw new RuntimeException(e.getMessage());
                }
                break;
            case CREATE_FUNCTION:
                //这里指的是永久函数，临时函数在会话中，不进元数据
                throw new MetaException("禁止CREATE_FUNCTION.");
            case DROP_FUNCTION:
                throw new MetaException("禁止DROP_FUNCTION.");

            //下面这几个 是 read 操作 放过
            case READ_TABLE:
                break;
            case READ_DATABASE:
                break;
            case READ_ISCHEMA:
                break;
            case READ_SCHEMA_VERSION:
                break;
            case READ_CATALOG:
                break;

            //未知操作 先放过
            case LOAD_PARTITION_DONE:
                break;
            case AUTHORIZATION_API_CALL:
                break;

            //禁止后续生成数据库的元数据信息库
            case CREATE_ISCHEMA:
                throw new MetaException("禁止CREATE_ISCHEMA.");
            case ALTER_ISCHEMA:
                throw new MetaException("禁止ALTER_ISCHEMA.");
            case DROP_ISCHEMA:
                throw new MetaException("禁止DROP_ISCHEMA.");
            case ADD_SCHEMA_VERSION:
                throw new MetaException("禁止ADD_SCHEMA_VERSION.");
            case ALTER_SCHEMA_VERSION:
                throw new MetaException("禁止ALTER_SCHEMA_VERSION.");
            case DROP_SCHEMA_VERSION:
                throw new MetaException("禁止DROP_SCHEMA_VERSION.");
            //catalog操作 禁止
            case ALTER_CATALOG:
                throw new MetaException("禁止ALTER_CATALOG.");
            case CREATE_CATALOG:
                throw new MetaException("禁止CREATE_CATALOG.");
            case DROP_CATALOG:
                throw new MetaException("禁止DROP_CATALOG.");
        }
    }
}
