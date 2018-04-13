package com.kylin.assembly.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @description: es客户端，基于transport方式
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-12
 **/
public class EsClient {

    private static Logger LOGGER = LoggerFactory.getLogger(EsClient.class);

    private static Client client;

    public static Client getEsClient()
    {
        if (client == null)
        {
            Settings settings = Settings.builder().put("cluster.name", "mycluster")
                    .put("client.transport.ping_timeout","30s")
                    //.put("transport.type", "netty3").put("http.type", "netty3")
                    .build();
            try
            {
                client = new PreBuiltTransportClient(settings).addTransportAddresses(
                        new InetSocketTransportAddress(InetAddress.getByName("liebao99.test.com"), 9300),
                        new InetSocketTransportAddress(InetAddress.getByName("liebao49.test.com"), 9300)
                );
            } catch (UnknownHostException e)
            {
                e.printStackTrace();
            }
        }
        return client;
    }

    public static void close()
    {
        if(client != null)
        {
            LOGGER.info("client closed..");
            client.close();
        }

    }
}
