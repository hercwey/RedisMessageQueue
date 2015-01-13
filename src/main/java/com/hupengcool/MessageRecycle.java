package com.hupengcool;

import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * 这个类功能和TaskMonitor类一样，这里采用的是Guava的Service框架来简化服务的生命周期管理。
 * Created by hupeng on 2015/1/6.
 */
public class MessageRecycle extends AbstractScheduledService{

    private static Log log = LogFactory.getLog(MessageRecycle.class);

    private RedisQueue redisQueue;

    private static final int DEFAULT_INTERVAL = 3 * 60;

    private int interval = DEFAULT_INTERVAL;

    public MessageRecycle() {
    }

    public MessageRecycle(RedisQueue redisQueue) {
        this.redisQueue = redisQueue;
    }

    public void setRedisQueue(RedisQueue redisQueue) {
        this.redisQueue = redisQueue;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    @Override
    protected void runOneIteration() throws Exception {
        try {
            redisQueue.checkExpireMessage();
        } catch (Exception e) {
            //handle Exception
            log.error(e);
        }

    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(5, interval, TimeUnit.SECONDS);
    }
}
