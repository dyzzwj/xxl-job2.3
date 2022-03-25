package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    public void init() throws Exception {
        // init i18n
        //初始化国际化
        initI18n();

        // admin trigger pool start
        /**
         *  启动两个线程池 一块一慢
         * 默认情况下，会使用fastTriggerPool。如果1分钟窗口期内任务耗时达500ms超过10次，则该窗口期内判定为慢任务，慢任务自动降级进入”Slow”线程池，避免耗尽调度线程，提高系统稳定性；
         */

        JobTriggerPoolHelper.toStart();

        // admin registry monitor run
        // 2. 启动注册监控器（将注册到register表中的IP加载到group表）/ 30执行一次
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run
        // 3. 启动失败日志监控器（失败重试，失败邮件发送）

        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        //丢失监控器启动
        JobCompleteHelper.getInstance().start();

        // admin log report start
        //日志报告启动
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )
        // 5. 启动定时任务调度器（执行任务，缓存任务）
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    /**
     * I18nUtil.getString方法就是根据配置读取resources/il8n/目录下的其中一个文件，
     * 该目录下有message_en.properties、message_zh_CN.properties、message_zh_TC.properties三个文件，
     * 分别为英语、中文简体、中文繁体是属性文件。I18nUtil.getString方法获取到执行阻塞策略的值赋值给title.
     *
     */
    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
