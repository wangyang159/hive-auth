package com.wy.utils;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 这类是用来对字段鉴权的工具类
 */
public class SqlFieldAuthCheckUtil {

    /**
     * 鉴权的核心方法
     * @param tableFieldMap 表为key，需鉴权字段List为value的一个map集合
     * @param username 当前任务的提交人，也就是打开会话的用户
     * @param executor 用来并行任务的线程池对象
     * @param mysqlUtil 并行任务查询权限库的连接池对象
     * @throws Exception 这里先抛出了一个总的异常，因为调用这里的时候也是抛出去打断鉴权，没有其他的处理要求
     */
    public static void checkPermissions(Map<String, List<String>> tableFieldMap, String username,ExecutorService executor,MysqlUtil mysqlUtil) throws HiveAuthzPluginException {
        /*
        1-1
        errorOccurred通知其他并行任务，是否触发没有权限异常的标志，使得其他任务如果在刚开始阶段就不在继续了

        AtomicBoolean 是java提供的线程安全布尔类型，但它并不是线程锁实现的，而是CAS+volatile实现

        好处有三点
             1、AtomicBoolean应用在多线程下需要一个布尔型的标识变量时，优先使用，它没有线程锁会造成的性能开销
                但并不是所有的相关类都是这样，比如 AtomicReference 用在引用类型多线程安全上，但是执行效率和开发成本就不一定比锁靠谱

             2、AtomicBoolean的值获取方法由volatile修饰符自身特性支持
                说白了内部其实是一个 volatile 修饰的int变量，如果想要了解运行逻辑强烈建议看一下源码

                在java中当一个变量被 volatile 所修饰，那么这个变量就会有两个能力
                第1个是它只要被改动，那么它自身的最新值会被直接写入主内存
                    并且无论是主线程还是其他子线程，对它的缓存都将失效，再次调用它时会强制从主内存中获取最新值
                    这样就确保了所有调用它的线程对于最新值的可见性

                这里有一个很容易疑惑的点：如果你尝试多线程下直接读写一个volatile变量会发现，虽然它自己被改变时其他线程缓存会失效
                但是最总任然会发生线程安全问题。
                对于这一点，你可以理解为：它确实会让其他地方的缓存失效，但是更新它的操作是否是原子性的它自己就没有办法保障了
                相当于它顾及了屁股(别入读它)，但是没管头(别人咋写的)

                第2个是它将带有屏障能力，可以通俗的理解为线程锁类似的东西，但目的不同。
                    这个能力存在的目的只是让jvm不会优化 volatile变量 在读写执行时，前后所有操作的顺序，实际作用很微弱
                    白话说就是 volatile变量 被读写之前的操作不会在它本身被读写之后执行，反过来同样
                    例如 a = 1 ; volatile变量 = 2 ; b = 3 ; 这时 a 和 b 的读写绝对在volatile变量的各自前后
                    因此不是用来解决多线程安全问题的

             3、AtomicBoolean更新方法依靠 volatile自身特性 以及 CAS实现(java提供的Unsafe类) 其实就是上了一个双重保险
                CAS实现是为了确保操作的原子性
                本身上是通过cpu级别的原子指令操作，使得操作本身就是不可被拆分的，说白话就是一个线程操作时不会让另一个线程插一脚
                但必须是原子级别的指令才有效，通俗的讲就是 a = 1 这种,如果是 a++ 这种包含原值读取、增加、最后赋值的操作就不是原子级别的了
                国内Java8的主要使用背景下，了解一下就行，尽量不要尝试实现其他的自定义CAS实现，因为非jvm提供的类之外调用Unsafe会触发安全限制
                从而遇到 java.lang.SecurityException: Unsafe 异常
                等你花大精力解决完安全限制，很可能还没有锁来的靠谱

             所以综合来说，多线程且非自带Atomic能解决的场景下，不会用volatile，而是去用锁
        */
        AtomicBoolean errorOccurred = new AtomicBoolean(false);

        //1-2 存放所有并行鉴权任务的回调对象
        List<Future<?>> futures = new ArrayList<>(tableFieldMap.entrySet().size());

        try {
            // 2-1 for循环遍历表-字段map，提交所有鉴权任务
            for (Map.Entry<String, List<String>> entry : tableFieldMap.entrySet()) {
                //2-2 如果前面提交到线程池的任务检查出了权限问题，就没有必要再继续提交后续的鉴权任务了
                if (errorOccurred.get()) {
                    break;
                }

                //2-3 提交鉴权任务,并保存回调对象
                String table = entry.getKey();
                List<String> fields = entry.getValue();
                futures.add(executor.submit( () -> {
                    //2-4 如果其他提交到线程池的任务，在当前线程任务开始开始前检查出了权限问题，就没有必要在执行了
                    if (errorOccurred.get()) {
                        return;
                    }

                    try {
                        // 2-4 提交鉴权子任务
                        checkAuth(table,fields,executor,errorOccurred,mysqlUtil,username);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage());
                    }
                } ));
            }

            // 3-1 前面的循环提交完所有任务，或遇到需要停止的标记 errorOccurred.get为 true时(此时会抛出中断异常)，程序会执行到此处执行到此，通过回调对象来做对应的操作
            for (Future<?> future : futures) {
                /*
                3-2 get方法会阻塞当前主进程，从而等待子线程结束，得到一个子线程回调结果
                    当然这个结果是啥不重要，重要的是所有子线程执行完，可以在上面提交任务中
                    改成用java.util.concurrent.CountDownLatch但是效果是一样的
                */
                future.get();
            }

        } catch (Exception e){
            // 保险起见，调用一下数据库连接池的资源回收已经分配出去的连接
            mysqlUtil.closeAllConnection();
            // 子线程任务中鉴权或者是其他任何异常，停止线程池的所有任务，释放资源
            for (Future<?> future : futures) {
                future.cancel(true);
            }
            futures.clear();

            //异常传递
            if (e instanceof HiveAuthzPluginException) {
                throw (HiveAuthzPluginException) e;
            }else {
                throw new HiveAuthzPluginException(e.getMessage());
            }
        }finally {
            // 所有鉴权任务的相关流程执行完成，释放所有连接池和线程池的资源
            mysqlUtil.closeAllConnection();
            futures.clear();
        }
    }

    /**
     * 2-4 控制执行单个鉴权任务的方法
     * @param table 表
     * @param fields 字段集合
     * @param executor
     * @param errorOccurred
     * @param mysqlUtil
     * @param username
     * @throws HiveAuthzPluginException
     */
    private static void checkAuth(String table, List<String> fields, ExecutorService executor, AtomicBoolean errorOccurred, MysqlUtil mysqlUtil,String username) throws HiveAuthzPluginException {
        /* 2-5
         获取是否外部原因需要中断任务，interrupted在获取中断标识之后
         会把已有的中断状态设置为默认为false，其实本身是一种中断信号的接力棒，如果上游发出中断要求
         这里获取并操作相关的事宜后，要把中断信号恢复成默认的，不影响整个流程其他框架获取

         errorOccurred.get 必须在这里检查是否整个鉴权任务不需要再执行了
         因为按照现有的逻辑来说，由于线程池任务提交很快，或者由于时间差等极端情况
         其他线程发出errorOccurred=true的同时或随机其后，任然有很大可能被调起n个线程任务

         这里要解释一下为什么用return，而不是抛出异常，是因为多线程执行环境下有一个异常会被吞掉
         */
        if (Thread.interrupted() || errorOccurred.get()) {
            return;
            //throw new InterruptedException("鉴权子流程响应中断 终止于 "+table+" 鉴权列表 "+fields);
        }

        // 2-6 得到并处理鉴权结果
        StringBuilder sql = new StringBuilder();

        // 用 StringBuilder 而不用 ？ 替换 是因为字段集合需要拼接，直接使用占位符会出现问题
        sql.append("select a.field,a.auth_flag,a.last_time ")
                .append("from db_tb_auth a inner join db_tb_info b on a.db_tb_id=b.db_tb_id ")
                .append("inner join user_info c on a.user_id=c.user_id ")
                .append("where c.user_name='").append(username).append("' and b.db_tb_name='").append(table).append("' and a.field in (");

        for (String f:fields) {
            sql.append("'").append(f).append("',");
        }

        //删掉最后的一个 ,
        sql.delete(sql.length()-1,sql.length());

        sql.append(") and a.last_time>=NOW() and a.auth_flag>=1 ");

        Connection connection = null;
        //是否发生权限异常，不用上面的errorOccurred是因为不能达到触发预期
        boolean auth_err_flag = false;
        try {
            connection = mysqlUtil.getConnection(true);
            ResultSet resultSet = connection.prepareStatement(sql.toString(),
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery();

            //复用stringbuilder，这个字符串缓冲器在子任务内，不会有线程安全问题
            sql.delete(0,sql.length());
            //如果返回的已有权限不为空集合,也就是last方法返回了true，并且最后一行的行号和查询字段数不一样，说明权限不够，则拼接出已有的权限列
            resultSet.last();//后期改造不要把这个方法放在if体里面，会无法命中，不过它运行之后没必要判断它的返回值，用行号判断就足够了
            if ( resultSet.getRow() != fields.size()){
                auth_err_flag = true;
                resultSet.beforeFirst();//让resultSet的指针回到表头位置
                sql.append("[ ");
                while (resultSet.next()){
                    sql.append(resultSet.getString("field")).append(" ");
                }
                sql.append("]");
                //更新线程池任务状态，如果是false就更新为true
                errorOccurred.compareAndSet(false, true);
                throw new HiveAuthzPluginException("字段鉴权 - 用户:"+username+" 对"+table+" 没有足够的权限，访问字段："+fields+" 已有权限："+sql.toString());
            }
            //除此之外权限正常通过

        } catch (SQLException e) {
            //发生sql异常的时候，和字段鉴权异常一样，定制所有的任务，并保障后面资源回收表示正常，不过一般不会正常不会发生这个异常
            errorOccurred.compareAndSet(false, true);
            auth_err_flag = true;
            throw new HiveAuthzPluginException("字段鉴权 - 鉴权库查询异常 "+e.getMessage());
        } finally {
            //关闭本次任务用的数据库连接
            mysqlUtil.closeConnection(connection);
            // 如果出现权限报错则此处回收所有数据库连接池线程资源，至于任务并行的线程池在最外层监听任务的地方回收
            // 这样在执行流程上可以使得连接池在此处回收完成后，上面的任务并行线程池再开始最后释放线程任务
            if (auth_err_flag){
                mysqlUtil.closeAllConnection();
            }
        }

    }

}