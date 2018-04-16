package com.kylin.assembly.strom;

import java.util.Arrays;


import backtype.storm.spout.SchemeAsMultiScheme;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;



public class StormHandler {
	
	public SpoutConfig spoutConf = null;
	protected String topic = null;
    private static Logger LOG = LoggerFactory.getLogger(StormHandler.class);

	
	public StormHandler(String topic){
		this.topic = topic;
		//本地
		//BrokerHosts brokerHosts = new ZkHosts(ConfigUtil.getParamHadoop("kafka.zookeeper.connect"));
		//亚信BrokerHosts
		ZkHosts brokerHosts = new ZkHosts(ConfigUtil.getParamHadoop("kafka.zookeeper.connect"),ConfigUtil.getParamHadoop("kafka.zkRoot")+ "/brokers");
		spoutConf = new SpoutConfig(brokerHosts, topic, ConfigUtil.getParamHadoop("kafka.zkRoot"), "rule"+topic);
		LOG.info("spoutConf参数---brokerHosts:"+brokerHosts+";topic:"+topic+";zkRoot:"+ConfigUtil.getParamHadoop("kafka.zkRoot")+";id"+"rule"+topic);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
     
        
		String[] servers = ConfigUtil.getParamHadoop("kafka.zookeeper.connect").split(":" + ConfigUtil.getParamHadoop("kafka.zookeeper.port"));
		spoutConf.zkServers = Arrays.asList(servers);
		spoutConf.zkPort = Integer.parseInt(ConfigUtil.getParamHadoop("kafka.zookeeper.port"));
		System.out.println("spoutConf.startOffsetTime:"+spoutConf.startOffsetTime);
      //  spoutConf.ignoreZkOffsets = false;
        spoutConf.startOffsetTime = 0L;// -2l 从kafka头开始 -1l 是从最新的开始 0l =无 从ZK开始,解决重复读的关键
        LOG.info("zkPort:"+spoutConf.zkPort);
        LOG.info("clientId:"+spoutConf.clientId);
        System.out.println("ignoreZkOffsets:"+spoutConf.ignoreZkOffsets);
        
        System.out.println("after:spoutConf.startOffsetTime:"+spoutConf.startOffsetTime);
	}
	
	
}