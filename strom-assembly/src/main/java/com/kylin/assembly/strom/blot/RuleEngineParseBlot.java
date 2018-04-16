package com.kylin.assembly.strom.blot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;



import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @Description	解析blot
 * @ClassName	RuleEngineParseBlot
 * @Date		2016年5月19日 上午11:31:58
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 */
public class RuleEngineParseBlot extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	
	private static final Logger LOG = LoggerFactory.getLogger(RuleEngineParseBlot.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    
	@Override
	public void execute(Tuple tuple) {
		System.out.println("start..parse");
        /*String messageList = tuple.getString(0);
        Map<String, String> kafkaMap=RuleBase.formateMessage(messageList);;
        *//*  if (1==1) {
    	System.out.println("数据清理");
    	collector.ack(tuple);
		return;
	    }*//*
        if (kafkaMap.size()==0) {
        	collector.ack(tuple);
			return;
		}
        Map<String, String> dataSourceInfo = RuleBase.findDataSource(kafkaMap);
        if (dataSourceInfo.size()==0) {
        	System.out.println("Redis_RTA_CORE_DATA_RESOURCE_INFO中没有"+kafkaMap.get("topic_head"));
        	collector.ack(tuple);
        	return;
        }
        //匹配RowKey
        ReceiveBean bean = RuleBase.combineRowKey(dataSourceInfo,kafkaMap);

        try {
			HbaseHelper.copyMessage(bean.getRowKey(),bean);
		} catch (Exception e) {
			LOG.error("保存接收到数据出错"+e.getMessage());
			 MapEntry<String, Object> params = new MapEntry<String, Object>();
			 params.put("error_info",e.getMessage());
			 params.put("msg",messageList);
			 params.put("step_id",2);
			try {
				Producer.doProcess(Constant.ERROR_TOPIC,JSONObject.toJSONString(params));
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				LOG.error("保存kafka都出错"+e1.getMessage());
			}
			collector.ack(tuple);
		}
        List<Map<String, String>> campaignList = RuleBase.findActivatesCampaigns(kafkaMap);
        if (campaignList.size()!=0) {
       	 for (Map<String, String> startMap : campaignList) {
       		 String campaign_id=startMap.get("CAMPAIGN_ID");
        		//每个活动各自所有流程
        		LOG.info("kafkaMap中放入campaign_id:"+campaign_id);
				Map<String, String> _kafkaMap=new HashMap<String, String>(kafkaMap);

				_kafkaMap.put("campaign_id",campaign_id);
				_kafkaMap.put("send_kafka_topic", startMap.get("SEND_KAFKA_TOPIC"));
       		List<Map<String, String>> processList = RuleBase.findCampaignProcess(campaign_id);
       		collector.emit(tuple, new Values(processList,_kafkaMap));
       		collector.ack(tuple);
       	 }
		 }else{
			 collector.ack(tuple);
		 }*/
	}


	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		outputFieldsDeclarer.declare(new Fields("processList","_kafkaMap"));
	}
	
}