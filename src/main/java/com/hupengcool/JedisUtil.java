package com.hupengcool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by hupeng on 2014/10/24.
 */
public class JedisUtil {

    public static JedisPoolConfig createPoolConfig(int maxTotal,int maxIdle) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMaxTotal(maxTotal);
        return poolConfig;
    }


    public static RedisTemplate createRedisTemplate(JedisPoolConfig config,String host,int port) {
        JedisPool jedisPool = new JedisPool(config,host,port);
        return new RedisTemplate(jedisPool);
    }


    public static void closeJedis(Jedis jedis) {
        if (jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                }
                jedis.disconnect();
            } catch (Exception e) {

            }
        }
    }
}
