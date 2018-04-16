package com.kylin.assembly.strom;



/**
 * 
 * @Description	各种常量
 * @ClassName	Constant
 * @Date		2016年5月20日 下午3:54:37
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 */
public class Constant {
	
	//isDev 
	public static final boolean isDev=Boolean.getBoolean(ConfigUtil.getParamArgument("isDev"));
     //topology namae
	public static final String TOPOLOGY_NAME=ConfigUtil.getParamArgument("topology_name");
	//发送 kafka_topic  
	public static final String KAFKA_TOPIC=ConfigUtil.getParamArgument("kafka_topic");
	//接收的kafka_topic
	public static final String receivceTopic1=ConfigUtil.getParamArgument("receivceTopic1");
	public static final String receivceTopic2=ConfigUtil.getParamArgument("receivceTopic2");
	public static final String receivceTopic3=ConfigUtil.getParamArgument("receivceTopic3");
	public static final String ERROR_TOPIC=ConfigUtil.getParamArgument("error_kafka");

	public static final int WORK_NUM=Integer.parseInt(ConfigUtil.getParamArgument("topology_work_num"));
	public static final Number SPOUT1_NUM=Integer.parseInt(ConfigUtil.getParamArgument("topology_spout1_num"));
	public static final Number SPOUT2_NUM=Integer.parseInt(ConfigUtil.getParamArgument("topology_spout2_num"));
	public static final Number SPOUT3_NUM=Integer.parseInt(ConfigUtil.getParamArgument("topology_spout3_num"));
	public static final Number PARSE_BLOT_NUM=Integer.parseInt(ConfigUtil.getParamArgument("topology_parseblot_num"));
	public static final Number DEAL_BLOT_NUM=Integer.parseInt(ConfigUtil.getParamArgument("topology_dealblot_num"));
	
	
}
