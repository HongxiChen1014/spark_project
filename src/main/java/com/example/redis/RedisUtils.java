package com.example.redis;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
    private static JedisPool jedisPool = null;
    private static final String HOST = "localhost";
    private static final int PORT = 6379;

    public static synchronized Jedis getJedis(){

        if(null == jedisPool) {
            GenericObjectPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(10);
            config.setMaxTotal(100);
            config.setMaxWaitMillis(1000);
            config.setTestOnBorrow(true);

            jedisPool = new JedisPool(config, HOST, PORT);
        }

        return jedisPool.getResource();
    }
}
