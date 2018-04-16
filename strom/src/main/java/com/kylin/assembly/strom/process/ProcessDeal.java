package com.kylin.assembly.strom.process;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;

import com.tydic.frame.kafka.Producer;
import com.tydic.rta.ruleEngine.Constant;
import com.tydic.rta.ruleEngine.blot.RuleEngineDealBlot;
import com.tydic.ruleEngine.po.MapEntry;
import com.tydic.ruleEngine.po.ReceiveBean;
import com.tydic.ruleEngine.storm.RuleBase;
import com.tydic.ruleEngine.util.HbaseHelper;


/**
 * @Description	TODO
 * @ClassName	ProcessDeal
 * @Date		2016年11月28日 下午4:06:52
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 */

public class ProcessDeal {
	
	private static final Log LOG = LogFactory.getLog(ProcessDeal.class);

	/**
	 * 
	 * @Description 处理流程
	 * @param 规则参数
	 * @param kafka中参数
	 * @return void 返回类型
	 * @throws Exception 
	 */
	public static void deal(List<Map<String, String>> processList, Map<String, String> kafkaMap) throws Exception {
		// TODO Auto-generated method stub
		SnProcessImpl ruleEngineAction=new SnProcessImpl();
		for (Map<String, String> processMap : processList) {
			Boolean	isContinue=ruleEngineAction.analysisRule(processMap,kafkaMap);
			if (!isContinue) {
		    	LOG.info("流程终止");
		    	break ;
			}
		}
		LOG.info("所有流程跑完");
	}
	
	public static void mainDeal(String messageList){
		
		  Map<String, String> kafkaMap=RuleBase.formateMessage(messageList);;
	        if (kafkaMap.size()==0) {
				return;
			}
	        Map<String, String> dataSourceInfo = RuleBase.findDataSource(kafkaMap);
	        if (dataSourceInfo.size()==0) {
	        	System.out.println("Redis_RTA_CORE_DATA_RESOURCE_INFO中没有"+kafkaMap.get("topic_head"));
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
	       		try {
					ProcessDeal.deal(processList,_kafkaMap);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println(e.getMessage());
				}
	       	 }
	        }
		
	}
}
