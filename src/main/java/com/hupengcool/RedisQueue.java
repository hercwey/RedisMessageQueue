package com.hupengcool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.Set;

/**
 * User: hupeng
 * Date: 14-10-22
 * Time: 下午8:30
 */
public class RedisQueue {

    private String doing;

    private String todo;

    private String toCheck;

    private RedisTemplate redisTemplate;
    private String queue;

    private static final int DEFALT_MAX_PROCESS_TIME = 2 * 60 * 1000;  //2 minute; 一个任务最大的处理时间

    private int maxProcessTime = DEFALT_MAX_PROCESS_TIME;

    public RedisQueue(RedisTemplate redisTemplate, String queue) {
        this.queue = queue;
        doing = "queue:" + queue + ":doing";
        todo = "queue:" + queue + ":todo";
        toCheck = "queue:" + queue + ":toCheck";
        this.redisTemplate = redisTemplate;
        this.queue = queue;
    }

    public void setMaxProcessTime(int maxProcessTime) {
        this.maxProcessTime = maxProcessTime;
    }

    /**
     * send message at the tail.
     * @param message
     */
    public void sendMessage(final String message) {
        if (message == null || message.length() == 0) {
            throw new IllegalArgumentException("message should not be null or empty");
        }
        redisTemplate.execute(new CallBack<Object>() {
            @Override
            public Object doInRedis(Jedis jedis) {
                jedis.lpush(todo, message);
                return null;
            }
        });
    }

    /**
     * send message at the head ,and it could be processed immediately
     * @param message
     */
    public void sendMessageToProcess(final String message) {
        redisTemplate.execute(new CallBack<Object>() {
            @Override
            public Object doInRedis(Jedis jedis) {
                 jedis.rpush(todo, message);
                return null;
            }
        });

    }

    /**
     * blocking
     * @param timeout  time for blocking
     * @return
     */
    public String askForMessage(final int timeout) {
       return redisTemplate.execute(new CallBack<String>() {
            @Override
            public String doInRedis(Jedis jedis) {
                String message = jedis.brpoplpush(todo, doing, timeout);
                if (message != null) {
                    jedis.zadd(toCheck, System.currentTimeMillis(), message);
                }
                return message;
            }
        });


    }

    /**
     * non blocking
     * @return
     */
    public String askForMessage() {
        return redisTemplate.execute(new CallBack<String>() {
            @Override
            public String doInRedis(Jedis jedis) {
                String message = jedis.rpoplpush(todo, doing);
                if (message != null) {
                    jedis.zadd(toCheck, System.currentTimeMillis(), message);
                }
                return message;
            }
        });


    }

    /**
     * check for expired message ,and put it into todo list..
     */
    public void checkExpireMessage() {
        redisTemplate.execute(new CallBack<Object>() {
            @Override
            public Object doInRedis(Jedis jedis) {
                Set<String> sets = jedis.zrangeByScore(toCheck, 0, System.currentTimeMillis() - maxProcessTime);
                Transaction t = jedis.multi();
                for (String message : sets) {
                    t.lrem(doing,0,message);
                    t.lpush(todo,message);
                    t.zrem(toCheck,message);
                }
                t.exec();
                return null;
            }
        });

    }


    /**
     * after message processed successfully
     * @param message
     */
    public void onCompleted(final String message) {
        redisTemplate.execute(new CallBack<Object>() {
            @Override
            public Object doInRedis(Jedis jedis) {
                Transaction t = jedis.multi();
                t.lrem(doing,0,message);
                t.zrem(toCheck,message);
                t.exec();
                return null;
            }
        });

    }

    public String getQueueName() {
        return queue;
    }
}

