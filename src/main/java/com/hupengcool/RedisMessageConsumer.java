package com.hupengcool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by hupeng on 2014/10/23.
 */
public class RedisMessageConsumer {
    private static Log log = LogFactory.getLog(RedisMessageConsumer.class);
    private RedisQueue redisQueue;
    private MessageListener listener;
    private ExecutorService exec = null;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean closed = new AtomicBoolean(false);

    public RedisMessageConsumer(RedisQueue redisQueue, MessageListener listener) {
        this.redisQueue = redisQueue;
        this.listener = listener;
    }

    private String getMessage() {
        return redisQueue.askForMessage();
    }

    private String getMessage(int timeOut) {
        return redisQueue.askForMessage(timeOut);
    }

    public void consume() {
        String message = getMessage(2); //brpoplpush 2s
        try {
            if (message != null && message.length() > 0) {
                listener.onMessage(message);
                redisQueue.onCompleted(message);
            } else {
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void start() {
         if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("this consumer already start!");
        }
        if (exec == null) {
            exec = Executors.newSingleThreadExecutor();
        }
        exec.submit(new Runnable() {
            @Override
            public void run() {
                while (!closed.get()) {
                    consume();
                }
            }
        });
    }


    public void close() throws IOException {
       if (closed.compareAndSet(false, true)) {
            throw new IllegalStateException("already close!");
        }

        if (exec != null) {
            ThreadUtils.shutdownAndAwaitTermination(exec, 10, TimeUnit.SECONDS);
        }

    }
}
