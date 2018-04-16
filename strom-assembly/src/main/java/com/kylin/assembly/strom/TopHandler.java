package com.kylin.assembly.strom;

import java.io.FileNotFoundException;
import java.util.Arrays;

import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;


/**
 * storm消息处理引擎
 * @package com.tydic.storm.storm.TopHandler.java
 * @title 
 * @description 
 * @author jiangyingxu
 * @date 2016-2-29
 * @version V1.0
 */
public abstract class TopHandler {
	
	protected static SpoutConfig spoutConf = null;
	protected String topic = null;
	
	public TopHandler(String topic){
		this.topic = topic;
		BrokerHosts brokerHosts = new ZkHosts(ConfigUtil.getParamHadoop("kafka.zookeeper.connect"));
		spoutConf = new SpoutConfig(brokerHosts, topic, ConfigUtil.getParamHadoop("kafka.zkRoot"), "rule_engine");
		
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		String[] servers = ConfigUtil.getParamHadoop("kafka.zookeeper.connect").split(":" + ConfigUtil.getParamHadoop("kafka.zookeeper.port"));
		spoutConf.zkServers = Arrays.asList(servers);
		spoutConf.zkPort = Integer.parseInt(ConfigUtil.getParamHadoop("kafka.zookeeper.port"));
	}
	
	/**
	 * 业务调用方法
	 * @description 
	 * @author jiangyingxu
	 * @create 2016-3-4
	 * @throws InterruptedException
	 * @throws FileNotFoundException
	 * @throws AuthorizationException
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 */
	public abstract void invokeBusiness() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException;
}