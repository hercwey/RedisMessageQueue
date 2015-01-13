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
    private volatile boolean close = false;
    private volatile boolean start = false;

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

    public synchronized void start() {
        if (start == true) {
            throw new IllegalStateException("this consumer already start!");
        }
        if (exec == null) {
            exec = Executors.newSingleThreadExecutor();
        }
        exec.submit(new Runnable() {
            @Override
            public void run() {
                while (!close) {
                    consume();
                }
            }
        });
        start = true;
    }


    public synchronized void close() throws IOException {
        if (close == true) {
            throw new IllegalStateException("already close!");
        }
        close = true;

        if (exec != null) {
            ThreadUtils.shutdownAndAwaitTermination(exec, 10, TimeUnit.SECONDS);
        }

    }
}
