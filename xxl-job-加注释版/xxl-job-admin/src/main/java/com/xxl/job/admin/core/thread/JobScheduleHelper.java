package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();
    public static JobScheduleHelper getInstance(){
        return instance;
    }

    public static final long PRE_READ_MS = 5000;    // pre read

    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    /**
     * 看似简单的一个任务触发为什么要搞这么复杂呢？
     * 我的答案是:  因为 出于“性能” 和“时效性”这两点 综合来考虑，即“中庸之道”。
     * 就拿每次 “从DB查出 近期 即将要到触发时间任务” 这个场景 来看：
     * 1  如果希望“性能”更好，那肯定每次多查出些数据，但这样就不可避免的造成 因为任务过多，同一批查出来的位置靠后的某些任务 触发就可能会延迟，比如实际触发比设定触发的时间晚几秒。
     * 2 如果希望“时效性”更好，那肯定每次少查出些数据，比如每次只查出来一条或者几条，实际触发时间和设定的触发时间 基本一样，但这样造成了频繁查询数据库，性能下降。
     * 故 通过“时间轮”来达到既“性能”比较好并且每次查出相对尽量多 的数据（目前是取5s内触发的任务），又通过时间轮来保障“时效性”：实际触发时间和设定的触发时间 尽量一样。这就是设计这么复杂的原因
     */
    public void start(){

        // schedule thread
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // 休眠最多5秒，调度线程会处理5秒内的任务
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");
                // pre-read count: threadpool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)
                // 预读取任务条数计算：(触发器线程池的最快处理数量200 + 触发器线程池的最慢处理数量100) * 20 ,其中最快处理数和最慢处理数都是application.properties配置的默认值
                // 默认 preReadCount = (200 + 100) * 20 = 6000
                int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;
                // 循环即将开始
                while (!scheduleThreadToStop) {
                    // 扫描作业任务
                    // Scan Job
                    long start = System.currentTimeMillis();
                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;
                    boolean preReadSuc = true;
                    try {
                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        // 开始事务
                        connAutoCommit = conn.getAutoCommit();
                        // 默认的话为自动提交，
                        //每执行一个update ,delete或者insert的时候都会自动提交到数据库，无法回滚事务。
                        //设置connection.setautocommit(false);只有程序调用connection.commit()的时候才会将先前执行的语句一起提交到数据库，这样就实现了数据库的事务。
                        //true：sql命令的提交（commit）由驱动程序负责
                        //false：sql命令的提交由应用程序负责，程序必须调用commit或者rollback方法
                        conn.setAutoCommit(false);
                        // select … for update 语句是我们经常使用手工加锁语句。在数据库中执行select … for update ,
                        // 大家会发现会对数据库中的表或某些行数据进行锁表，在mysql中，如果查询条件带有主键，会锁行数据，如果没有，会锁表。
                        // 这里是表锁，where的条件使用的是主键，为行锁，如果非主键，则为表锁
                        // for update是悲观锁
                        // 赤裸裸的分布式锁
                        preparedStatement = conn.prepareStatement(  "select * from xxl_job_lock where lock_name = 'schedule_lock' for update" );
                        preparedStatement.execute();
                        // tx start
                        // 1、pre read
                        long nowTime = System.currentTimeMillis();
                        // 获取下一次执行时间内的，约定的最大要读取的任务数
                        // 轮询db，找出trigger_next_time（下次触发时间）在距now 5秒内的任务
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        if (scheduleList!=null && scheduleList.size()>0) {
                            // 2、push time-ring
                            // 迭代搂出来的任务列表
                            for (XxlJobInfo jobInfo: scheduleList) {
                                // ***触发算法***
                                // 拿到了距now 5秒内的任务列表数据：scheduleList，分三种情况处理：for循环遍历scheduleList集合
                                //（1）对到达now时间后的任务：（超出now 5秒外）：直接跳过不执行； 重置trigger_next_time；
                                //（2）对到达now时间后的任务：（超出now 5秒内）：线程执行触发逻辑； 若任务下一次触发时间是在5秒内，
                                // 则放到时间轮内（Map<Integer, List<Integer>> 秒数(1-60) => 任务id列表）；
                                // 再 重置trigger_next_time
                                //（3）对未到达now时间的任务：直接放到时间轮内；重置trigger_next_time
                                //~----------------------------------------------
                                // 如果当前时间大于(下次触发时间+预读取时间5秒),那么更新任务表中的下次触发时间，并且不执行任务直接跳过
                                // 因为说明这些任务的预计执行时间过了，相当于过期了，赶不上这趟车，那给他更新下下次的触发时间点，让他在后面的车次中执行
                                // time-ring jump
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                                    logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());
                                    // 更新任务表中的下次触发时间
                                    // fresh next
                                    refreshNextValidTime(jobInfo, new Date());
                                // 如果当前时间大于下次触发时间点
                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                                    // *** 关键点&重点：执行触发器
                                    // 1、trigger
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId() );
                                    // 更新下次触发点时间
                                    // 2、fresh next
                                    refreshNextValidTime(jobInfo, new Date());
                                    // 当前作业状态为正在执行且（当前时间+5秒）大于下次触发点时间
                                    // next-trigger-time in 5s, pre-read again
                                    if (jobInfo.getTriggerStatus()==1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                                        // 生成秒环
                                        // 1、make ring second
                                        // jobInfo.getTriggerNextTime()/1000 把下次触发时间点转换成秒
                                        // (jobInfo.getTriggerNextTime()/1000)%60 把下次触发的秒值浓缩成60秒的环形秒环上
                                        // key是hash计算触发时间获得的秒数(1-60)，value是任务id列表
                                        // 入轮：扫描任务触发时 （1）本次任务处理完成，但下一次触发时间是在5秒内（2）本次任务未达到触发时间
                                        // 出轮：获取当前时间秒数，从时间轮内移出当前秒数前2个秒数的任务id列表， 依次进行触发任务；（避免处理耗时太长，跨过刻度，多向前校验一秒）
                                        // 增加时间轮的目的是：任务过多可能会延迟，为了保障触发时间尽可能和 任务设置的触发时间尽量一致，把即将要触发的任务提前放到时间轮里，每秒来触发时间轮相应节点的任务
                                        // 这个思路虽然复杂，但是设计的很精巧且牛逼
                                        int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);
                                        // 生成时间环，一圈60等分，每一份1秒，每一份挂着一个list，每个list里存的是这一秒要触发的任务id:jobId
                                        // 2、push time ring
                                        pushTimeRing(ringSecond, jobInfo.getId());
                                        // 更新下次触发点时间
                                        // 3、fresh next
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                    }

                                } else {
                                    // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time

                                    // 1、make ring second
                                    int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                    // 2、push time ring
                                    pushTimeRing(ringSecond, jobInfo.getId());

                                    // 3、fresh next
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                }

                            }
                            // 3、update trigger info
                            for (XxlJobInfo jobInfo: scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }
                        } else {
                            preReadSuc = false;
                        }
                        // tx stop
                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {
                        // commit
                        if (conn != null) {
                            try {
                                // 事务提交
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            // 关闭事务
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                        // close PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                    long cost = System.currentTimeMillis()-start;
                    // Wait seconds, align second
                    if (cost < 1000) {  // scan-overtime, not wait
                        try {
                            // pre-read period: success > scan each second; fail > skip this period;
                            TimeUnit.MILLISECONDS.sleep((preReadSuc?1000:PRE_READ_MS) - System.currentTimeMillis()%1000);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        // 设置成守护线程
        scheduleThread.setDaemon(true);
        // 设置线程名
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        // 启动线程
        scheduleThread.start();

        // ***时间轮数据结构***：
        // Map<Integer, List<Integer>>  key是hash计算触发时间获得的秒数(1-60)，value是任务id列表
        // 入轮：扫描任务触发时 （1）本次任务处理完成，但下一次触发时间是在5秒内（2）本次任务未达到触发时间
        // 出轮：获取当前时间秒数，从时间轮内移出当前秒数前2个秒数的任务id列表， 依次进行触发任务；（避免处理耗时太长，跨过刻度，多向前校验一秒）
        // 增加时间轮的目的是：任务过多可能会延迟，为了保障触发时间尽可能和 任务设置的触发时间尽量一致，
        // 把即将要触发的任务提前放到时间轮里，每秒来触发时间轮相应节点的任务
        // ring thread
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // align second
                try {
                    TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!ringThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }

                while (!ringThreadToStop) {
                    try {
                        // second data
                        List<Integer> ringItemData = new ArrayList<>();
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);   // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
                        for (int i = 0; i < 2; i++) {
                            List<Integer> tmpData = ringData.remove( (nowSecond+60-i)%60 );
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }
                        }

                        // ring trigger
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData) );
                        if (ringItemData.size() > 0) {
                            // do trigger
                            for (int jobId: ringItemData) {
                                // do trigger
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }

                    // next second, align second
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis()%1000);
                    } catch (InterruptedException e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    /**
     * 更新下一次任务执行触发时间点
     * 1、先通过CronExpression用cron规则和当前时间计算出下次触发时间点
     * 2、如果计算出来的这个时间点不为空，就给jobInfo赋值
     * 3、如果计算出来的这个时间点为空，就给jobInfo的相关属性置成0
     * @param jobInfo
     * @param fromTime
     * @throws ParseException
     */
    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws ParseException {
        Date nextValidTime = new CronExpression(jobInfo.getJobCron()).getNextValidTimeAfter(fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
        }
    }

    /**
     * Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>()
     *  key是hash计算触发时间获得的秒数(1-60)，value是任务id列表
     * 入轮：扫描任务触发时 （1）本次任务处理完成，但下一次触发时间是在5秒内（2）本次任务未达到触发时间
     * 出轮：获取当前时间秒数，从时间轮内移出当前秒数前2个秒数的任务id列表， 依次进行触发任务；（避免处理耗时太长，跨过刻度，多向前校验一秒）
     * 增加时间轮的目的是：任务过多可能会延迟，为了保障触发时间尽可能和 任务设置的触发时间尽量一致，
     * 把即将要触发的任务提前放到时间轮里，每秒来触发时间轮相应节点的任务
     * 这个思路虽然复杂，但是设计的很精巧且牛逼
     * @param ringSecond
     * @param jobId
     */
    private void pushTimeRing(int ringSecond, int jobId){
        // push async ring
        List<Integer> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Integer>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData) );
    }

    public void toStop(){

        // 1、stop schedule
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData!=null && tmpData.size()>0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }

}
