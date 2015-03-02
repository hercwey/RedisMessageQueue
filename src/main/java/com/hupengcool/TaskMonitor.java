package com.hupengcool;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by hupeng on 2014/10/23.
 */
public class TaskMonitor {
    private RedisQueue redisQueue;

    private static final int DEFAULT_INTERVAL = 3 * 60;

    private int interval = DEFAULT_INTERVAL;

    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean closed = new AtomicBoolean(false);

    private ScheduledExecutorService scheduler = null;


    public TaskMonitor(RedisQueue redisQueue) {
        this.redisQueue = redisQueue;
    }

    public TaskMonitor() {
    }

    public void setRedisQueue(RedisQueue redisQueue) {
        this.redisQueue = redisQueue;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public void start() {
         if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("this TaskMonitor already start!");
        }
        if (scheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
        }

        ScheduledFuture future = scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    redisQueue.checkExpireMessage();
                } catch (Exception e) {
                    //handle Exception
                }
            }
        }, 5, interval, TimeUnit.SECONDS);

    }

    public synchronized void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            throw new IllegalStateException("this TaskMonitor already close!");
        }

        if (scheduler != null) {
            ThreadUtils.shutdownAndAwaitTermination(scheduler, 5, TimeUnit.SECONDS);
        }
    }
}
