package com.hupengcool;

/**
 * Created by hupeng on 2014/10/23.
 */
public class MessageProducer {
    private RedisQueue redisQueue;

    public MessageProducer(RedisQueue redisQueue) {
        this.redisQueue = redisQueue;
    }

    public void sendMessage(String message) {
        redisQueue.sendMessage(message);
    }

    public void sendMessageToProcess(String message) {
        redisQueue.sendMessageToProcess(message);
    }
}
