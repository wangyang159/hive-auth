当前插件已实现的能力如下表

| 功能点                      | 当前插件实现                                                                                                          | 二次改造代码地点                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| hive原生自带授权语句             | 屏蔽                                                                                                              | MyHiveAuthorization.checkPrivileges                        |
| 普通查询，或结果集建表、结果集插入表       | 当前用户是否为落盘表owner，否则直接拒绝，内部对下游权限对象操作做了判断，因此结果集建表并不会触发不在预期内的权限检查<br/><br/>当前用户是否为查询表owner，否则进行字段级鉴权                | MyHiveAuthorization.checkPrivileges                        |
| 查询表是否允许越过字段              | 不允许select语句不携带任何字段，例如select count(1) from a                                                                     | MyHiveAuthorization.checkPrivileges                        |
| 清空表数据                    | 当前用户是否为操作表的owner，否则直接拒绝                                                                                         | MyHiveAuthorization.checkPrivileges                        |
| 展示表或库资源列表                | 并没有做特别过滤，目前是hive返回什么就展示什么                                                                                       | MyHiveAuthorization.filterListCmdObjects                   |
| 展示单张表的详情信息，也就是DESCTABLE时 | 需要owner权限                                                                                                       | MyHiveAuthorization.checkPrivileges                        |
| 建表执行前                    | 视图不做限制外，表名和字段名长度要符合外部鉴权库数据长度限制这个是在代码中写死的，所以该了鉴权库的表结构后需要改代码<br/><br/>预留了路径的校验，可以扩展路径格式等<br/><br/>不对Paimon表坐路径的校验 | MyMetaStorePreEventListener.onEvent.CREATE_TABLE           |
| 建表成功后                    | 预留了扩展                                                                                                           | MyMetaStoreEventListener.onCreateTable                     |
| 改表结构执行前                  | 视图不做限制外，非owner不能改表结构<br/><br/>库名不能改<br/><br/>分区表不允许改表location<br/>预留了其他不能改表限制的位置                                | MyMetaStorePreEventListener.onEvent.ALTER_TABLE            |
| 改表结构成功后                  | 视图不做限制外，已删除的表字段权限回收(hive不允许直接删除字段，但运行调整字段顺序时缺省字段来达到删除目的)<br/><br/>预留允许的location变更之后干什么                          | MyMetaStoreEventListener.onAlterTable                      |
| 删除表执行前                   | 检查是否是owner，不是则拒绝                                                                                                | MyMetaStorePreEventListener.onEvent.DROP_TABLE             |
| 删除表成功后                   | 回收外部权限库中的权限信息                                                                                                   | MyMetaStoreEventListener.onDropTable                       |
| 新增表分区执行前                 | 不操作视图和路径在元数据服务中未知的表<br/><br/>新增的分区路径不能在表路径之外                                                                    | MyMetaStorePreEventListener.onEvent.ADD_PARTITION          |
| 新增表分区成功后                 | 预留了扩展代码                                                                                                         | MyMetaStoreEventListener.onAddPartition                    |
| 删除表分区执行前                 | 检查是否是owner                                                                                                      | MyMetaStorePreEventListener.onEvent.DROP_PARTITION         |
| 删除表分区成功后                 | 预留扩展代码                                                                                                          | MyMetaStoreEventListener.onDropPartition                   |
| 更改表分区执行前                 | 阻止视图和alluxio meta的表被操作                                                                                          | MyMetaStorePreEventListener.onEvent.ALTER_PARTITION        |
| 更改表分区成功后                 | 预留扩展代码                                                                                                          | MyMetaStoreEventListener.onAlterPartition                  |
| 更改库信息执行前                 | 拒绝所有库信息的更改                                                                                                      | MyMetaStorePreEventListener.onEvent.ALTER_DATABASE         |
| 建库执行前                    | 必须携带库路径                                                                                                         | MyMetaStorePreEventListener.onEvent.CREATE_DATABASE        |
| 删库执行前                    | 拒绝所有删库操作                                                                                                        | MyMetaStorePreEventListener.onEvent.DROP_DATABASE          |
| 创建永久函数执行前                | 拒绝创建永久函数，只能使用add语句用临时函数                                                                                         | MyMetaStorePreEventListener.onEvent.CREATE_FUNCTION        |
| 删除永久函数执行前                | 拒绝删除已有的永久函数                                                                                                     | MyMetaStorePreEventListener.onEvent.DROP_FUNCTION          |
| 手动生成数据库的元数据信息库前          | 禁止                                                                                                              | MyMetaStorePreEventListener.onEvent.CREATE_ISCHEMA 开始的六个操作类型 |
| catalog操作                | 禁止                                                                                                              | MyMetaStorePreEventListener.onEvent.ALTER_CATALOG 开始的三个操作类型 |


注意，该插件对于外部鉴权库中的数据，主要是读取，表owner相关权限并不是来自于鉴权库，而是依赖hive元数据服务中存储的owner ，不会对表owner的鉴权走外部权限数据库
除了删表和删字段的权限回收之外，不会有写入操作，因此对于丰富的赋权场景，需要自行准备外部权限系统，将权限信息写入权限库即可


<hr/>

使用方式：

1、拉取源码，并更改pom中hadoop和hive版本为具体使用版本，默认是hadoop3.2.3 + hive3.1.3，之后编译
```bash
git clone https://github.com/wangyang159/hive-auth.git
cd hive-auth
mvn -DskipTests=true clean package
```
结果hivePlu.jar，会出现在target目录下，将它放入hive的lib目录中

2、将下面的内容存在就修改到hive-site.xml中

```
<property>
    <name>hive.security.authorization.enabled</name>
    <value>true</value>
</property>

<property>
    <name>hive.security.authorization.manager</name>
    <value>com.wy.auth.MyHiveAuthorizationFactory</value>
</property>

<property>
    <name>hive.metastore.event.listeners</name>
    <value>com.wy.meta.MyMetaStoreEventListener</value>
</property>

<property>
   <name>hive.metastore.pre.event.listeners</name>
   <value>com.wy.meta.MyMetaStorePreEventListener</value>
</property>
```

3、打开hive-log4j

找到已有的 loggers 并在后面追加三个插件用的日志类
```properties
#原：
#loggers = NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, PerfLogger, AmazonAws, ApacheHttp

#追加后：
loggers = NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, PerfLogger, AmazonAws, ApacheHttp, MyHiveAuth, MyMetaStore, MyMetaPreStore
```

另起一行追加如下内容
```properties
logger.MyHiveAuth.name = com.wy.auth.MyHiveAuthorizationFactory
logger.MyHiveAuth.level = INFO

logger.MyMetaStore.name = com.wy.meta.MyMetaStoreEventListener
logger.MyMetaStore.level = INFO

logger.MyMetaPreStore.name = com.wy.meta.MyMetaStorePreEventListener
logger.MyMetaPreStore.level = INFO
```

4、整体上采用外部鉴权数据，因此下面需要配置鉴权数据库，在并准备好的鉴权库中执行如下语句，生成权限表

```sql
/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50725
 Source Host           : localhost:3306
 Source Schema         : shop

 Target Server Type    : MySQL
 Target Server Version : 50725
 File Encoding         : 65001

 Date: 14/06/2025 15:57:38
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for db_tb_auth
-- ----------------------------
DROP TABLE IF EXISTS `db_tb_auth`;
CREATE TABLE `db_tb_auth`  (
                               `auth_id` int(4) NOT NULL AUTO_INCREMENT COMMENT '权限表主键',
                               `db_tb_id` int(4) NULL DEFAULT NULL COMMENT '表信息主键',
                               `user_id` int(4) NULL DEFAULT NULL COMMENT '用户信息主键',
                               `field` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '权限信息的主体字段',
                               `auth_flag` int(2) NULL DEFAULT NULL COMMENT '权限标识： 1 读取权限 ; 0 无任何权限',
                               `last_time` datetime(0) NULL DEFAULT NULL COMMENT '权限的到期时间',
                               PRIMARY KEY (`auth_id`) USING BTREE,
                               INDEX `表信息外键`(`db_tb_id`) USING BTREE,
                               INDEX `用户信息外键`(`user_id`) USING BTREE,
                               INDEX `表字段索引`(`field`) USING BTREE,
                               CONSTRAINT `用户信息外键` FOREIGN KEY (`user_id`) REFERENCES `user_info` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
                               CONSTRAINT `表信息外键` FOREIGN KEY (`db_tb_id`) REFERENCES `db_tb_info` (`db_tb_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 6 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for db_tb_info
-- ----------------------------
DROP TABLE IF EXISTS `db_tb_info`;
CREATE TABLE `db_tb_info`  (
                               `db_tb_id` int(4) NOT NULL AUTO_INCREMENT COMMENT '表信息id',
                               `db_tb_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '库名_表名',
                               PRIMARY KEY (`db_tb_id`) USING BTREE,
                               UNIQUE INDEX `表名称索引`(`db_tb_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user_info
-- ----------------------------
DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info`  (
                              `user_id` int(4) NOT NULL AUTO_INCREMENT COMMENT '用户信息主键',
                              `user_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '用户名',
                              PRIMARY KEY (`user_id`) USING BTREE,
                              UNIQUE INDEX `用户名称索引`(`user_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- 删除表触发的存储过程
-- ----------------------------
DELIMITER //
CREATE PROCEDURE DeleteTableAndAuth(IN in_db_tb_name VARCHAR(100))
BEGIN
    DECLARE in_db_tb_id INT;
    DECLARE auth_deleted INT DEFAULT 0;
    DECLARE info_deleted INT DEFAULT 0;
    
    -- 查找表ID
    SELECT db_tb_id INTO in_db_tb_id
    FROM db_tb_info
    WHERE db_tb_name = in_db_tb_name
    LIMIT 1;

    -- 如果找到表
    IF in_db_tb_id IS NOT NULL THEN
        START TRANSACTION;
        
        -- 删除权限记录
        DELETE FROM db_tb_auth
        WHERE db_tb_id = in_db_tb_id;
        SET auth_deleted = ROW_COUNT();
        
        -- 删除表信息
        DELETE FROM db_tb_info
        WHERE db_tb_id = in_db_tb_id;
        SET info_deleted = ROW_COUNT();

        COMMIT;

        -- 返回删除统计
        SELECT
            in_db_tb_name AS table_name,
            auth_deleted AS auth_records_deleted,
            info_deleted AS info_records_deleted;
    ELSE
        -- 返回未找到信息
        SELECT
            in_db_tb_name AS table_name,
            'Table not found' AS message;
    END IF;
END //
DELIMITER ;

SET FOREIGN_KEY_CHECKS = 1;
```
5、将权限库的连接信息，写在hive的hive-site.xml文件中

```
<property>
    <name>hive.auth.database.url</name>
    <value>jdbc:mysql://192.168.0.110:3306/shop?useUnicode=true&amp;characterEncoding=UTF-8&amp;serverTimezone=Asia/Shanghai</value>
</property>

<!-- 这里用的是mysql5.x的连接驱动，且maven依赖为provided，如果你用的是mysql8或者其他情况自己编译或者直接在hive的lib下替换 -->
<property>
    <name>hive.auth.database.driver</name>
    <value>com.mysql.jdbc.Driver</value>
</property>

<!-- 链接超时 默认5秒 -->
<property>
    <name>hive.auth.database.timeout</name>
    <value>5000</value>
</property>

<property>
    <name>hive.auth.database.username</name>
    <value>root</value>
</property>

<property>
    <name>hive.auth.database.password</name>
    <value>123456</value>
</property>

<!-- 设置最大连接数 默认 10 ，不可超过无符号Int范围，按照每个hive会话内的一次sql运行来决定就行
因为鉴权类的生命周期就是一个会话提交第一个sql开始到连接断开，而鉴权流程是每一个select-sql执行都会有一个线程池和连接池并行鉴权-->
<property>
    <name>hive.auth.database.hikari.pool.maxsize</name>
    <value>10</value>
</property>

<!-- 设置最小空闲连接数 默认 2 ，不可超过总大小的一半 向下取整 -->
<property>
    <name>hive.auth.database.hikari.pool.minidle</name>
    <value>2</value>
</property>

<!-- 设置空闲超时时间 默认30秒(30000) -->
<property>
    <name>hive.auth.database.hikari.pool.idle.timeout</name>
    <value>30000</value>
</property>

<!-- 设置连接最大存活时间 默认9分钟(540000) 不能小于空闲超时时间 或 超过无符号Long范围 -->
<property>
    <name>hive.auth.database.hikari.pool.max.lifetime</name>
    <value>540000</value>
</property>
```
