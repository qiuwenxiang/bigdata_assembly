package com.kylin.assembly.redis;

import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.CommandListener;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @description:
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-05-19
 **/
public class RedisCopy {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Replicator replicator = new RedisReplicator("redis:///D://appendonly.aof");
        replicator.addCommandListener(new CommandListener() {
            @Override
            public void handle(Replicator replicator, Command command) {

                System.out.println(command);
            }
        });
        replicator.open();
    }
}
