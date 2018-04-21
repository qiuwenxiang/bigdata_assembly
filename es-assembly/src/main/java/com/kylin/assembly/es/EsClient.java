package com.kylin.assembly.es;

import com.kylin.assembly.common.GlobalParamValue;
import com.kylin.assembly.common.constant.EsConstant;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
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
            Settings settings = Settings.builder().put("cluster.name", GlobalParamValue.get(EsConstant.CLUSTER_NAME))
                    .put("client.transport.ping_timeout","30s")
                    //.put("transport.type", "netty3").put("http.type", "netty3")
                    .build();
            try
            {
                PreBuiltTransportClient preBuiltTransportClient= new PreBuiltTransportClient(settings);
                String[] ips = GlobalParamValue.get(EsConstant.ESIP).split(",");
                TransportAddress[] arr=new TransportAddress[ips.length];
                for (int i=0;i<ips.length;i++){
                    arr[i]= new InetSocketTransportAddress(InetAddress.getByName(ips[i]), Integer.parseInt(GlobalParamValue.get(EsConstant.ESPORT)));
                }
                client = preBuiltTransportClient.addTransportAddresses(arr);
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
