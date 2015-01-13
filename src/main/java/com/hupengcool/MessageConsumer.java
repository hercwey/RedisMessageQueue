package com.hupengcool;


import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 这个类功能和RedisMessageConsumer类一样，这里采用的是Guava的Service框架来简化服务的生命周期管理。
 * Created by hupeng on 2015/1/6.
 */
public class MessageConsumer extends AbstractExecutionThreadService {

    private static Log log = LogFactory.getLog(MessageConsumer.class);

    private volatile boolean RUNNING = true;

    private RedisQueue redisQueue;

    private MessageListener listener;

    public MessageConsumer(RedisQueue redisQueue,MessageListener listener) {
        this.redisQueue = redisQueue;
        this.listener = listener;
    }

    @Override
    protected void run() throws Exception {
        while (RUNNING) {
            try {
                String message = redisQueue.askForMessage(2); //brpoplpush 2s
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

    }

    @Override
    protected void triggerShutdown() {
        RUNNING = false;
    }

}
