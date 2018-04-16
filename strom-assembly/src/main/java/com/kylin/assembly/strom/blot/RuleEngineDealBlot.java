package com.kylin.assembly.strom.blot;

import java.util.List;
import java.util.Map;

import com.kylin.assembly.strom.Constant;
import com.oracle.jrockit.jfr.Producer;
import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @Description	处理blot
 * @ClassName	RuleEngineDealBlot
 * @Date		2016年5月19日 上午11:31:45
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 */
public class RuleEngineDealBlot extends BaseRichBolt{

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	
	private static final Logger LOG = LoggerFactory.getLogger(RuleEngineDealBlot.class);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    
	@Override
	public void execute(Tuple arg0) {
		System.out.println("start..deal");
		List<Map<String, String>> processList = (List<Map<String, String>>) arg0.getValueByField("processList");
		/*Map<String, String> kafkaMap = (Map<String, String>)arg0.getValueByField("_kafkaMap");
        try {
        	ProcessDeal.deal(processList,kafkaMap);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("出错"+e.getMessage());
			 JSONObject json=new JSONObject();
			 json.put("processList", processList);
			 json.put("kafkaMap", kafkaMap);
			 System.out.println(json.toJSONString());
			 
			 MapEntry<String, Object> params = new MapEntry<String, Object>();
			 params.put("error_info",e.getMessage());
			 params.put("msg",json.toJSONString());
			 params.put("step_id",3);
			
			try {
				Producer.doProcess(Constant.ERROR_TOPIC,JSONObject.toJSONString(params));
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				LOG.error("保存kafka都出错"+e1.getMessage());
			}
		}finally{
			collector.ack(arg0);
		}*/
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}
	
}