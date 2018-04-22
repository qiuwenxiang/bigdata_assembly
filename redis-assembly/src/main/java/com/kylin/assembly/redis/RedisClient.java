package com.kylin.assembly.redis;

import com.kylin.assembly.common.GlobalParamValue;
import com.kylin.assembly.common.constant.RedisConstant;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author kylin.qiuwx@foxmail.com
 * @Description: 基于redis 3.0连接客户端
 * @date 2018/4/22
 */
public class RedisClient {

    private static JedisCluster jc = null;

    /**
     * 初始化redis集群client
     */
    static {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        String[] iplist = GlobalParamValue.get(RedisConstant.CLUSTER_REDIS_NODES).split(",");
        String[] ls = null;
        for (String ipport : iplist) {
            ls = ipport.split(":");
            jedisClusterNodes.add(new HostAndPort(ls[0], Integer.valueOf(ls[1])));
        }
        jc = new JedisCluster(jedisClusterNodes, 30000, 1000);
        try {
            jc.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
