当前插件将hive自带的权限语句屏蔽掉了，采用外部鉴权数据，因此下面需要配置鉴权数据库

注意，该插件只使用了鉴权库中的数据，主要是读取，至于表owner的相关权限并不是来自于鉴权库
而是依赖hive元数据服务中存储的owner ，不对表owner做鉴权
因此不会发生任何写的操作，对于丰富的赋权场景，需要自行准备外部权限系统，将权限信息写入写入权限库即可

目前由于只控制到了读取权限相关操作，所以对于权限变动的操作，比如表被删除、新表是否给其他人默认权限、表字段更改
等等，诸如此类的变化操作需要按需更改源码

<hr/>

1、将下面的内容存在就修改到hive-site.xml中

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
2、将下面的内容追加到hive-log4j中
```properties
logger.hook1.name = com.wy.auth.MyHiveAuthorizationFactory
logger.hook1.level = INFO
logger.hook2.name = com.wy.meta.MyMetaStoreEventListener
logger.hook2.level = INFO
logger.hook3.name = com.wy.meta.MyMetaStorePreEventListener
logger.hook3.level = INFO
```
3、在准备好的鉴权库中执行如下语句，生成权限表
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
4、将权限库的连接信息，写在hive的hive-site.xml文件中
```
<property>
    <name>hive.auth.database.url</name>
    <value>jdbc:mysql://192.168.0.110:3306/shop?useUnicode=true&amp;characterEncoding=UTF-8&amp;serverTimezone=Asia/Shanghai</value>
</property>

<-- 这里用的是mysql5.x的连接驱动，如果你用的是mysql8就需要从自己编译一下 -->
<property>
    <name>hive.auth.database.driver</name>
    <value>com.mysql.jdbc.Driver</value>
</property>

<-- 链接超时 默认5秒 -->
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
