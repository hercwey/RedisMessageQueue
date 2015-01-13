package com.hupengcool;

import redis.clients.jedis.Jedis;

/**
 * Created by hupeng on 2014/10/23.
 */
public interface CallBack<T> {
    public T doInRedis(Jedis jedis);
}
