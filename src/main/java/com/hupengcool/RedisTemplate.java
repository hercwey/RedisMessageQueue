package com.hupengcool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Created by hupeng on 2014/10/23.
 */
public class RedisTemplate {

    private JedisPool jedisPool;

    private Log log = LogFactory.getLog(RedisTemplate.class);


    public RedisTemplate() {
    }

    public RedisTemplate(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public <T> T execute(CallBack<T> callBack) {
        Jedis jedis = jedisPool.getResource();
        try {
            T t = callBack.doInRedis(jedis);
            return t;
        } catch (JedisException e) {
            if (e instanceof JedisConnectionException) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            handleJedisException(e);
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }


    /**
     * Handle jedisException, write log and return whether the connection is broken.
     */
    protected void handleJedisException(JedisException jedisException) {
        if (jedisException instanceof JedisConnectionException) {
            log.error("Redis connection  lost.........", jedisException);
        } else if (jedisException instanceof JedisDataException) {
            if ((jedisException.getMessage() != null) && (jedisException.getMessage().indexOf("READONLY") != -1)) {
                log.error("Redis connection are read-only slave.", jedisException);
            }
            log.error("Jedis exception happen:" + jedisException);
        } else {
            log.error("Jedis exception happen.", jedisException);
        }
    }

    public void closeJedisPool () {
        if (jedisPool != null) {
            jedisPool.destroy();
        }
    }
}
