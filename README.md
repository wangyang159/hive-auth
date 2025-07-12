当前插件已实现的能力如下表

| 功能点                      | 当前插件实现                                                                                                                                                     | 二次改造代码地点                                                     |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| hive原生自带授权语句             | 屏蔽                                                                                                                                                         | MyHiveAuthorization.checkPrivileges                          |
| 普通查询，或结果集建表、结果集插入表       | 当前用户是否为落盘表owner，否则直接拒绝，内部对下游权限对象操作做了判断，因此结果集建表并不会触发不在预期内的权限检查<br/><br/>当前用户是否为查询表owner，否则进行字段级鉴权                                                           | MyHiveAuthorization.checkPrivileges                          |
| 查询表是否允许越过字段              | 不允许select语句不携带任何字段，例如select count(1) from a                                                                                                                | MyHiveAuthorization.checkPrivileges                          |
| 清空表数据                    | 当前用户是否为操作表的owner，否则直接拒绝                                                                                                                                    | MyHiveAuthorization.checkPrivileges                          |
| 展示表或库资源列表                | 并没有做特别过滤，目前是hive返回什么就展示什么                                                                                                                                  | MyHiveAuthorization.filterListCmdObjects                     |
| 展示单张表的详情信息，也就是DESCTABLE时 | 需要owner权限                                                                                                                                                  | MyHiveAuthorization.checkPrivileges                          |
| 建表执行前                    | 视图不做限制外，表名和字段名长度要符合外部鉴权库数据长度限制，这个是在代码中写死的，所以改鉴权库的表结构后需要改代码<br/><br/>除非是Paimon表不做路径的校验，其他情况外表会检查表路径是否已经被使用，无论内、外表location不能超过500个字符，和字段一样长度要和鉴权库中存储字段长度保持一致 | MyMetaStorePreEventListener.onEvent.CREATE_TABLE             |
| 建表成功后                    | 将表信息写入鉴权库<br/><br/>预留了扩展                                                                                                                                   | MyMetaStoreEventListener.onCreateTable                       |
| 改表结构执行前                  | 视图不做限制外，非owner不能改表结构<br/><br/>不允许变更库名和表名<br/><br/>不允许改表location<br/>预留了其他不能改表限制的位置                                                                         | MyMetaStorePreEventListener.onEvent.ALTER_TABLE              |
| 改表结构成功后                  | 视图不做限制外，已删除的表字段权限回收(hive不允许直接删除字段，但运行调整字段顺序时缺省字段来达到删除目的)<br/><br/>当表字段发生变动后同步维护鉴权库中的表字段列表<br/><br/>预留允许的location变更之后干什么                                    | MyMetaStoreEventListener.onAlterTable                        |
| 删除表执行前                   | 检查是否是owner，不是则拒绝                                                                                                                                           | MyMetaStorePreEventListener.onEvent.DROP_TABLE               |
| 删除表成功后                   | 回收外部权限库中的该表所有的权限、表信息                                                                                                                                       | MyMetaStoreEventListener.onDropTable                         |
| 新增表分区执行前                 | 不操作视图和路径在元数据服务中未知的表<br/><br/>新增的分区路径不能在表路径之外                                                                                                               | MyMetaStorePreEventListener.onEvent.ADD_PARTITION            |
| 新增表分区成功后                 | 预留了扩展代码                                                                                                                                                    | MyMetaStoreEventListener.onAddPartition                      |
| 删除表分区执行前                 | 检查是否是owner                                                                                                                                                 | MyMetaStorePreEventListener.onEvent.DROP_PARTITION           |
| 删除表分区成功后                 | 预留扩展代码                                                                                                                                                     | MyMetaStoreEventListener.onDropPartition                     |
| 更改表分区执行前                 | 阻止视图和alluxio meta的表被操作                                                                                                                                     | MyMetaStorePreEventListener.onEvent.ALTER_PARTITION          |
| 更改表分区成功后                 | 预留扩展代码                                                                                                                                                     | MyMetaStoreEventListener.onAlterPartition                    |
| 更改表分区存储路径                | 和表存储路径一样不允许                                                                                                                                                | MyHiveAuthorization.checkPrivileges                          |
| 更改库信息执行前                 | 拒绝所有库信息的更改，原因和表存储路径一样，具体见表存储变更检查代码中的注释                                                                                                                     | MyMetaStorePreEventListener.onEvent.ALTER_DATABASE           |
| 建库执行前                    | 必须携带库路径<br/><br/>库路径为了和表路径的500长度限制配合，所以限制了最长400个字符                                                                                                         | MyMetaStorePreEventListener.onEvent.CREATE_DATABASE          |
| 删库执行前                    | 拒绝所有删库操作                                                                                                                                                   | MyMetaStorePreEventListener.onEvent.DROP_DATABASE            |
| 创建永久函数执行前                | 拒绝创建永久函数，只能使用add语句用临时函数                                                                                                                                    | MyMetaStorePreEventListener.onEvent.CREATE_FUNCTION          |
| 删除永久函数执行前                | 拒绝删除已有的永久函数                                                                                                                                                | MyMetaStorePreEventListener.onEvent.DROP_FUNCTION            |
| 手动生成数据库的元数据信息库前          | 禁止                                                                                                                                                         | MyMetaStorePreEventListener.onEvent.CREATE_ISCHEMA 开始的六个操作类型 |
| catalog操作                | 禁止                                                                                                                                                         | MyMetaStorePreEventListener.onEvent.ALTER_CATALOG 开始的三个操作类型  |


注意，该插件对于外部鉴权库中的数据，主要是读取，表owner相关权限并不是来自于鉴权库，而是依赖hive元数据服务中存储的owner ，因此不会对表owner的鉴权走外部权限数据库
除了删表和删字段的权限回收，以及建表后的表信息写入之外，不会有写入操作，因此对于丰富的赋权场景，需要自行准备外部权限系统，将权限信息写入权限库即可


<hr/>

使用方式：

1、拉取源码，并更改pom中hadoop和hive版本为具体使用版本，默认是hadoop3.2.3 + hive3.1.3，之后编译
```bash
git clone https://github.com/wangyang159/hive-auth.git
cd hive-auth
mvn -DskipTests=true clean package
```
结果hive-auth*.jar，会出现在target目录下，将它放入hive的lib目录中

2、将下面的内容存在就修改，否则直接添加到hive-site.xml中

```
<!-- 受保护参数，不同的hive版本默认值不一样，下面这一长串3.1.3以下都不需要改 -->
<property>
    <name>hive.conf.restricted.list</name>
    <value>hive.spark.client.connect.timeout,hive.spark.client.server.connect.timeout,hive.spark.client.channel.log.level,hive.spark.client.rpc.max.size,hive.spark.client.rpc.threads,hive.spark.client.secret.bits,hive.spark.client.rpc.server.address,hive.spark.client.rpc.server.port,hikari.*,dbcp.*,hadoop.bin.path,hive.security.authorization.enabled,hive.security.authorization.manager,hive.metastore.event.listeners,hive.metastore.pre.event.listeners,hive.auth.database.url,hive.auth.database.driver,hive.auth.database.timeout,hive.auth.database.username,hive.auth.database.password,hive.auth.database.hikari.pool.maxsize,hive.auth.database.hikari.pool.minidle,hive.auth.database.hikari.pool.idle.timeout,hive.auth.database.hikari.pool.max.lifetime</value>
</property>

<property>
    <name>hive.security.authorization.enabled</name>
    <value>true</value>
</property>

<!-- sql鉴权组件 -->
<property>
    <name>hive.security.authorization.manager</name>
    <value>com.wy.auth.MyHiveAuthorizationFactory</value>
</property>

<!-- 元数据被操作后的鉴权插件 -->
<property>
    <name>hive.metastore.event.listeners</name>
    <value>com.wy.meta.MyMetaStoreEventListener</value>
</property>

<!-- 元数据被操作前的鉴权插件 -->
<property>
   <name>hive.metastore.pre.event.listeners</name>
   <value>com.wy.meta.MyMetaStorePreEventListener</value>
</property>
```

3、打开hive-log4j

找到已有的 loggers 并在后面追加三个插件用的日志类，后期在日志调试上，主要查看hive-log4j日志配置设置的目录，极少量的启动进程也会有日志，比如插件中类的调起，非常少需要的时候留意一下就行

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

4、整体上采用外部鉴权数据，因此下面需要配置鉴权数据库，并在准备好的鉴权库中执行如下语句，生成鉴权数据相关表

注意：需要至少 MySQL 5.5.3+ 的数据库

```sql
/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50725
 Source Host           : localhost:3306
 Source Schema         : hive_auth

 Target Server Type    : MySQL
 Target Server Version : 50725
 File Encoding         : 65001

 Date: 17/06/2025 21:17:54
*/
SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

CREATE DATABASE IF NOT EXISTS hive_auth CHARACTER SET utf8 COLLATE utf8_general_ci;
use hive_auth;

-- ----------------------------
-- 权限表
-- ----------------------------
DROP TABLE IF EXISTS `db_tb_auth`;
CREATE TABLE `db_tb_auth`  (
                               `auth_id` int(4) NOT NULL AUTO_INCREMENT COMMENT '权限表主键',
                               `db_tb_id` int(4) NULL DEFAULT NULL COMMENT '表信息主键',
                               `user_id` int(4) NULL DEFAULT NULL COMMENT '用户信息主键',
                               `field` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '权限信息的主体字段',
                               `auth_flag` int(2) NULL DEFAULT NULL COMMENT '权限标识： 1 读取权限 ; 0 无任何权限',
                               `last_time` datetime(0) NULL DEFAULT NULL COMMENT '权限的到期时间',
                               PRIMARY KEY (`auth_id`) USING BTREE,
                               INDEX `表信息外键`(`db_tb_id`) USING BTREE,
                               INDEX `用户信息外键`(`user_id`) USING BTREE,
                               INDEX `表字段索引`(`field`) USING BTREE,
                               CONSTRAINT `db_tb_auth_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `user_info` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
                               CONSTRAINT `db_tb_auth_ibfk_2` FOREIGN KEY (`db_tb_id`) REFERENCES `db_tb_info` (`db_tb_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- hive表信息
-- ----------------------------
DROP TABLE IF EXISTS `db_tb_info`;
CREATE TABLE `db_tb_info`  (
                               `db_tb_id` int(4) NOT NULL AUTO_INCREMENT COMMENT '表信息id',
                               `user_id` int(4) NULL DEFAULT NULL COMMENT 'owner-id',
                               `db_tb_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '库名.表名',
                               `tb_fields` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '表的字段列表英文逗号分割',
                               `tb_location` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '表存储路径',
                               PRIMARY KEY (`db_tb_id`) USING BTREE,
                               UNIQUE INDEX `表名称索引`(`db_tb_name`) USING BTREE,
                               UNIQUE INDEX `表存储路径索引`(`tb_location`) USING BTREE,
                               INDEX `表的owner`(`user_id`) USING BTREE,
                               CONSTRAINT `表的owner` FOREIGN KEY (`user_id`) REFERENCES `user_info` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 7 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- 用户信息表
-- ----------------------------
DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info`  (
                              `user_id` int(4) NOT NULL AUTO_INCREMENT COMMENT '用户信息主键',
                              `user_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '用户名',
                              PRIMARY KEY (`user_id`) USING BTREE,
                              UNIQUE INDEX `用户名称索引`(`user_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- 删表权限回收时用到的存储过程：传入 库.表名
-- 其中直接操作了删除和新增，是因为hive中库+表名的情况下是不可能重复的
-- hive自己的元数据服务就会做这方面的安全检查
-- ----------------------------
DROP PROCEDURE IF EXISTS `DeleteTableAndAuth`;
delimiter ;;
CREATE PROCEDURE `DeleteTableAndAuth`(IN in_db_tb_name VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci)
BEGIN
    -- 需要的变量：表id、删除了多少权限、删除了多少表消息
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
        START TRANSACTION;-- 因为涉及到删除，所以用事务

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
            concat('表:',in_db_tb_name) AS table_name,
            concat('回收表权限条数:',auth_deleted) AS auth_records_deleted,
            concat('回收表信息条数:',info_deleted) AS info_records_deleted;
    ELSE
        -- 返回未找到信息
        SELECT
            in_db_tb_name AS table_name,
            '不在预期内的异常 - 鉴权库中不存在该表信息' AS message;
    END IF;
END ;;
delimiter ;

-- ----------------------------
-- 建表后表信息写入的存储过程：传入 建表用户名 、 库.表名 、字段列表 、 表存储路径
-- 在这个存储过程中查询已有用户id查不到则返回-1，由程序内部去做逻辑返回
-- 而不是用户不存在则插入用户，是因为鉴权数据的维护并不应该，且正常也不会直接面向用户
-- 而是中间存在一个外部鉴权功能的系统，应该由这个系统以及整个技术环境统一维护所有用户相关的信息
-- 而不是鉴权模块自己维护，因此这里没有用户返回 -1 反之正常新增表消息，返回受影响行数
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertTableInfo`;
DELIMITER ;;
CREATE PROCEDURE `InsertTableInfo`(
    IN in_user_name VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci,
    IN in_db_tb_name VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci,
    IN in_tb_fields text CHARACTER SET utf8 COLLATE utf8_general_ci,
    IN in_tb_location VARCHAR(500) CHARACTER SET utf8 COLLATE utf8_general_ci
)
BEGIN
    -- 用户id 、 受影响行数
    DECLARE in_user_id INT;
    DECLARE in_affected_rows INT DEFAULT 0;

    -- 1. 查询用户ID
    SELECT user_id INTO in_user_id
    FROM user_info
    WHERE user_name = in_user_name
    LIMIT 1;

    -- 2. 用户不存在，则新增数据，并最终返回-1
    IF in_user_id IS NULL THEN
        INSERT INTO user_info (user_name) VALUES (in_user_name);
        SET in_user_id = LAST_INSERT_ID();
        INSERT INTO db_tb_info (user_id, db_tb_name, tb_fields, tb_location) VALUES (in_user_id, in_db_tb_name, in_tb_fields, in_tb_location);
        SELECT -1 AS result;
    ELSE
        -- 3. 存在插入数据表信息
        INSERT INTO db_tb_info (user_id, db_tb_name, tb_fields, tb_location) VALUES (in_user_id, in_db_tb_name, in_tb_fields, in_tb_location);

        -- 4. 获取并返回受影响行数
        SET in_affected_rows = ROW_COUNT();
        SELECT in_affected_rows AS result;
    END IF;
END ;;
DELIMITER ;

-- ----------------------------
-- ranger-hdfs鉴权时调用的存储过程，传入用户名 、访问路径 、 是否经过检查owner的逻辑，传入 0 则不检查owner
-- mysql 中 true(1) false(0) 和c语言是一样的非0为真
-- 返回 -1 说明当前路径不是一个表路径
-- 返回 0  说明是表路径，但权限不够 同时携带表名
-- 返回 1 则说明是表路径，且权限足够 同时携带表名
-- ----------------------------
DROP PROCEDURE IF EXISTS `RangerAuthCheck`;
delimiter ;;
CREATE PROCEDURE `RangerAuthCheck`(
    IN in_user_name VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci,
    IN in_path varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci,
    IN check_owner int
)
BEGIN
    -- 表id、表字段列表、表名称、表字段个数、拥有字段权限的个数、owner检查的结果 0 非owner 1 则是owner
    DECLARE in_db_tb_id INT;
    DECLARE in_tb_fields text;
    DECLARE in_db_tb_name varchar(100);
    DECLARE in_tb_fields_size INT;
    DECLARE in_have_tb_fields_size INT;
    DECLARE in_check_owner INT;

    -- 查找表ID
    SELECT
        db_tb_id,tb_fields,db_tb_name INTO in_db_tb_id,in_tb_fields,in_db_tb_name
    FROM db_tb_info FORCE INDEX(表存储路径索引)
    -- 路径的hdfs前缀写在了数据库中,预防有变化的时候,本质上是因为ranger鉴权中的路径失败是 / 开头的，而不是 hdfs:// 开头
    -- 其他鉴权组件内部用的都是hive处理好的 hdfs:// 开头的完整路径
    WHERE concat('hdfs://node1:9000',in_path) like concat(tb_location,'%')
    ORDER BY tb_location DESC
    LIMIT 1;

    -- 不是一个表路径，则返回 -1 表示不需要做字段相关的鉴权
    if in_db_tb_id is null then
        select -1 as result;
    else
        -- 查询这个表的owner，这是个防御写法，程序中调用时一般传入 0 并提前检查owner
        if check_owner = 1 then
            select b.user_name = in_user_name into in_check_owner
            from db_tb_info a inner join user_info b
                on a.user_id=b.user_id where a.db_tb_id=in_db_tb_id ;
            if in_check_owner = 1 then
                select 1 as result;-- 返回有权限的标识
            else
                -- 如果是表路径，也就是查询表id不空，且不是owner，则计算出这个表有几个字段
                select (length(in_tb_fields)-length(replace(in_tb_fields,',','')))+1 into in_tb_fields_size;
                -- 计算出这个用户有目的表多少个字段权限
                select
                    count(a.field) into in_have_tb_fields_size
                from db_tb_auth a
                         inner join user_info b on a.user_id=b.user_id
                where a.auth_flag>=1 and a.last_time>=now() 
                  and b.user_name=in_user_name and a.db_tb_id=in_db_tb_id;
                -- 表字段个数和已有权限字段格式是否相当
                select
                    in_tb_fields_size = in_have_tb_fields_size as result,
                    in_db_tb_name;
            end if ;-- 是否因 owner 直接返回
        else
            -- 如果是表路径，id不空，则计算出这个表有几个字段
            select (length(in_tb_fields)-length(replace(in_tb_fields,',','')))+1 into in_tb_fields_size;
            -- 计算出这个用户有目的表多少个字段权限
            select
                count(a.field) into in_have_tb_fields_size
            from db_tb_auth a
                     inner join user_info b on a.user_id=b.user_id
            where a.auth_flag>=1 and a.last_time>=now() 
              and b.user_name=in_user_name and a.db_tb_id=in_db_tb_id;
            -- 表字段个数和已有权限字段格式是否相当
            select
                in_tb_fields_size = in_have_tb_fields_size as result,
                in_db_tb_name;
        end if ;-- 这个结束的是是否检查owner
    end if ;-- 这个end if 结束的是最外面是否查到表的if语句
END ;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
```
5、将权限库的连接信息，写在hive的hive-site.xml文件中

```
<!-- 鉴权库的连接url -->
<property>
    <name>hive.auth.database.url</name>
    <value>jdbc:mysql://192.168.0.110:3306/hive_auth?useUnicode=true&amp;characterEncoding=UTF-8&amp;serverTimezone=Asia/Shanghai</value>
</property>

<!--
这里用的是mysql5.x的连接驱动，且maven依赖为provided
如果你用的是mysql8或者其他情况自己编译或者直接在hive的lib下替换
注意！！！一定要和hive元数据服务用同一个版本的driver，不然会类冲突
-->
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

<!-- 
设置鉴权任务并行上限 默认 10 ，不可超过无符号Int范围
按照每个hive会话内的一次sql运行来决定个数就行
鉴权类的生命周期就是一个会话提交第一个sql开始到连接断开
而操作数据的鉴权流程是这个会话内，每一个select-sql执行都会有一个持有存在的线程池和连接池并行鉴权
会话内的任务是依次执行的，用户不可能一个会话同时运行两个任务，所以按照一次sql任务的运行来决定个数就行
-->
<property>
    <name>hive.auth.database.authorizer.hikari.pool.maxsize</name>
    <value>10</value>
</property>

<!--
元数据权限监控组件运行在服务端的meta进程中
且内部同样采用持久化的线程池与鉴权库交互，因此作为服务级别的线程池大的个数要大一些
默认100
-->
<property>
    <name>hive.auth.database.meta.listener.hikari.pool.maxsize</name>
    <value>100</value>
</property>

<!-- 
设置最小空闲连接数 默认 2 ，不可超过总大小的一半 向下取整 
后期下面的所有参数，元数据鉴权池都会复用
-->
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
